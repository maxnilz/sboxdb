use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use sboxdb::error::Error;
use sboxdb::raft::node::NodeId;
use sboxdb::raft::{ApplyMsg, Command, Index, State};

type Log = HashMap<Index, Command>;
#[derive(Debug)]
pub struct States {
    logs: Mutex<Vec<Log>>,
}

impl States {
    pub fn new(n: u8) -> Self {
        let mut logs = vec![];
        for _ in 0..n {
            logs.push(HashMap::new());
        }
        Self { logs: Mutex::new(logs) }
    }

    fn apply(&self, id: NodeId, msg: ApplyMsg) -> sboxdb::error::Result<Command> {
        let mut logs = self.logs.lock().unwrap();

        // check if any server has already applied a different command at given index.
        let (index, command) = (msg.index, msg.command);
        for i in 0..logs.len() {
            let log = &logs[i];
            if let Some(cmd) = log.get(&index) {
                if command != *cmd {
                    #[rustfmt::skip]
                        let msg = format!("inconsistent command applied at {}, {}/{} {}/{}",
                                          index, id, command, i, *cmd);
                    return Err(Error::internal(msg));
                }
            }
        }

        // check if there is a gap, i.e., if prev index is applied or not
        let prev = index - 1;
        if prev > 0 {
            let log = &logs[id as usize];
            if !log.contains_key(&prev) {
                let msg = format!("server {} apply out of order at {}", id, index);
                return Err(Error::internal(msg));
            }
        }

        // apply the msg to log
        let log = &mut logs[id as usize];
        log.insert(msg.index, command.clone());

        Ok(command)
    }

    pub fn napplied(&self, index: Index) -> sboxdb::error::Result<(u8, Option<Command>)> {
        let logs = self.logs.lock().unwrap();
        let mut n = 0;
        let mut ans: Option<Command> = None;
        for log in logs.iter() {
            let cmd = log.get(&index);
            if cmd.is_none() {
                continue;
            }
            let cur = cmd.unwrap();
            #[rustfmt::skip]
            if let Some(prev) = &ans && n > 1 {
                if *prev != *cur {
                    let msg= format!("applied values do not match: index {}, {}, {}", index, *prev, *cur);
                    return Err(Error::internal(msg));
                }
            }
            n += 1;
            ans = Some(cur.clone());
        }
        Ok((n, ans))
    }
}

#[derive(Debug)]
pub struct KvState {
    id: NodeId,
    states: Arc<States>,
}

impl KvState {
    pub fn new(id: NodeId, states: Arc<States>) -> KvState {
        KvState { id, states }
    }
}

impl State for KvState {
    fn apply(&self, msg: ApplyMsg) -> sboxdb::error::Result<Command> {
        self.states.apply(self.id, msg)
    }
}
