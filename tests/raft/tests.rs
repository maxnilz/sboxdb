use std::sync::Arc;
use std::time::Duration;

use log::debug;
use log::info;
use rand::thread_rng;
use rand::Rng;
use sboxdb::error::Result;
use sboxdb::raft::node::NodeId;
use sboxdb::raft::Command;
use sboxdb::raft::CommandResult;

use super::cluster::max_election_timeout;
use super::cluster::Cluster;
use super::transport::Noise;

#[test]
fn test_initial_election_r1() -> Result<()> {
    let cases = vec![1, 2, 3, 4, 5, 6];
    for num_nodes in cases {
        setup!(cluster, num_nodes);

        info!("test initial election with cluster size {}", num_nodes);

        // check if a leader elected.
        let leader1 = cluster.check_one_leader()?;

        // check all servers agree on a same term
        let term1 = cluster.check_terms()?;

        // does the leader+term stay the same if there is no network failure?
        std::thread::sleep(max_election_timeout());
        // the term should be the same
        let term2 = cluster.check_terms()?;
        assert_eq!(term1, term2);
        // the leader should be the same
        let leader2 = cluster.check_one_leader()?;
        assert_eq!(leader1, leader2);

        teardown!(cluster);
    }

    Ok(())
}

#[test]
fn test_re_election_r1() -> Result<()> {
    let num_nodes = 3;
    setup!(cluster, num_nodes);

    let leader1 = cluster.check_one_leader()?;

    // if the leader disconnect, a new one should be elected.
    cluster.disconnect(leader1);
    let leader2 = cluster.check_one_leader()?;
    assert_ne!(leader2, leader1);

    // if old leader rejoins, that should not disturb the
    // new leader. and the old leader should switch to follower.
    cluster.connect(leader1);
    let leader3 = cluster.check_one_leader()?;
    assert_eq!(leader2, leader3);
    let ns = cluster.get_node_state(leader1);
    assert_eq!(Some(leader2), ns.leader);

    // if there is no quorum, no new leader should be elected.
    cluster.disconnect(leader2);
    cluster.disconnect(((leader2 as u8 + 1) % num_nodes) as NodeId);
    std::thread::sleep(max_election_timeout());

    // check that the one connected server does not think it is the leader.
    cluster.check_no_leader()?;

    // if quorum arise, it should elect a leader.
    cluster.connect(((leader2 as u8 + 1) % num_nodes) as NodeId);
    cluster.check_one_leader()?;

    // re-join of last node, should not prevent leader from existing.
    cluster.connect(leader2);
    cluster.check_one_leader()?;

    teardown!(cluster);
    Ok(())
}

#[test]
fn test_many_election_r1() -> Result<()> {
    let num_nodes = 7;
    setup!(cluster, num_nodes);

    let iters = 10;
    for i in 0..iters {
        debug!("test many election iter {}", i);

        // disconnect three nodes
        let i1 = thread_rng().gen_range(0..num_nodes) as NodeId;
        let i2 = thread_rng().gen_range(0..num_nodes) as NodeId;
        let i3 = thread_rng().gen_range(0..num_nodes) as NodeId;

        cluster.disconnect(i1);
        cluster.disconnect(i2);
        cluster.disconnect(i3);

        // either the current leader should alive, or
        // the remaining four should elect a new one.
        cluster.check_one_leader()?;

        cluster.connect(i1);
        cluster.connect(i2);
        cluster.connect(i3);
    }

    cluster.check_one_leader()?;

    teardown!(cluster);

    Ok(())
}

#[test]
fn test_election_over_noise_net_r1() -> Result<()> {
    setup!(cluster, 3, Noise::new(20, 100..500));

    // check if a leader elected.
    cluster.check_one_leader()?;

    // check all servers agree on a same term
    let term1 = cluster.check_terms()?;

    std::thread::sleep(max_election_timeout());
    // since we are in an unstable net, the term might
    // be the different, but shouldn't be backward.
    let term2 = cluster.check_terms()?;
    assert!(term1 <= term2);
    // we can still leader elected out.
    cluster.check_one_leader()?;

    teardown!(cluster);
    Ok(())
}

#[test]
fn test_basic_agree_r2() -> Result<()> {
    let num_nodes = 3;
    setup!(cluster, num_nodes);

    for index in 1..=3 {
        let (n, _) = cluster.napplied(index)?;
        assert_eq!(n, 0, "some have committed before");
        let got = cluster.one(vec![index as u8], num_nodes, false)?;
        assert_eq!(index, got, "got index {}, expected {}", got, index);
    }

    teardown!(cluster);

    Ok(())
}

#[test]
fn test_fail_agree_r2() -> Result<()> {
    // a follower participates first, then disconnect and reconnect.
    let num_nodes = 3;
    setup!(cluster, num_nodes);

    cluster.one(vec![0x0b], num_nodes, false)?;

    // disconnect one follower from the network.
    let leader = cluster.check_one_leader()?;
    cluster.disconnect((leader + 1) % num_nodes);

    // the leader and the reaming follower should be
    // able to agree despite the disconnected follower.
    cluster.one(vec![0x0c], num_nodes - 1, false)?;
    cluster.one(vec![0x0d], num_nodes - 1, false)?;
    std::thread::sleep(max_election_timeout());
    cluster.one(vec![0x0e], num_nodes - 1, false)?;
    cluster.one(vec![0x0f], num_nodes - 1, false)?;

    // reconnect the disconnected follower.
    cluster.connect((leader + 1) % num_nodes);

    // the full set of servers should preserve previous
    // agreements, and be able to agree on new commands.
    cluster.one(vec![0x10], num_nodes - 1, true)?;
    std::thread::sleep(max_election_timeout());
    cluster.one(vec![0x11], num_nodes - 1, true)?;

    teardown!(cluster);

    Ok(())
}

#[test]
fn test_fail_no_agree_r2() -> Result<()> {
    // no agreement if too many followers disconnect
    let num_nodes = 5;
    setup!(cluster, num_nodes);

    cluster.one(vec![0x01], num_nodes, false)?;

    // 3 of 5 followers disconnect.
    let leader = cluster.check_one_leader()?;
    cluster.disconnect((leader + 1) % num_nodes);
    cluster.disconnect((leader + 2) % num_nodes);
    cluster.disconnect((leader + 3) % num_nodes);

    // check no agreement can be made.
    let timeout = max_election_timeout();
    let res = cluster.exec_command(leader, vec![0x02], Some(timeout))?;
    assert_eq!(res, CommandResult::Ongoing(2), "should block on index #2");

    std::thread::sleep(max_election_timeout());

    let (m, _) = cluster.napplied(2)?;
    assert_eq!(m, 0, "should have no apply");

    // repair
    cluster.connect((leader + 1) % num_nodes);
    cluster.connect((leader + 2) % num_nodes);
    cluster.connect((leader + 3) % num_nodes);

    let leader = cluster.check_one_leader()?;
    let res = cluster.exec_command(leader, vec![0x03], Some(timeout))?;
    #[rustfmt::skip]
    assert_eq!(res, CommandResult::Applied { index: 2, result: Ok(vec![0x03].into()) }, "command should applied at index 2");

    cluster.one(vec![0x04], num_nodes, false)?;

    teardown!(cluster);

    Ok(())
}

#[test]
fn test_concurrent_cmd_r2() -> Result<()> {
    let num_nodes = 5;
    setup!(cluster, num_nodes);

    let leader = cluster.check_one_leader()?;
    let server = &cluster.server(leader);

    let mut expect = vec![];
    let mut threads = vec![];
    for i in 0..10 {
        expect.push(vec![i]);
        let server = Arc::clone(server);
        let th = std::thread::spawn(move || server.execute_command(Command::from(vec![i]), None));
        threads.push(th);
    }

    let mut got = vec![];
    for th in threads {
        let res = th.join().unwrap();
        if let Ok(CommandResult::Applied { result, .. }) = res {
            got.push(result?.unwrap());
        }
    }
    assert_eq!(expect.len(), got.len());

    got.sort();
    assert_eq!(expect, got);

    teardown!(cluster);

    Ok(())
}

#[test]
fn test_rejoin_r2() -> Result<()> {
    let num_nodes = 3;
    setup!(cluster, num_nodes);

    cluster.one(vec![0x01], num_nodes, false)?;

    // leader network failure
    let leader = cluster.check_one_leader()?;
    cluster.disconnect(leader);

    // make old leader try to agree on some entries
    cluster.exec_command(leader, vec![0x02], Some(Duration::from_millis(0)))?;
    cluster.exec_command(leader, vec![0x03], Some(Duration::from_millis(0)))?;
    cluster.exec_command(leader, vec![0x04], Some(Duration::from_millis(0)))?;

    // new leader commit in majority, also for index=2
    cluster.one(vec![0x05], num_nodes - 1, true)?;

    // new leader network failure, old leader connected.
    let leader1 = cluster.check_one_leader()?;
    cluster.disconnect(leader1);
    cluster.connect(leader);

    cluster.one(vec![0x06], num_nodes - 1, true)?;

    // all together now
    cluster.connect(leader1);

    cluster.one(vec![0x06], num_nodes, true)?;

    teardown!(cluster);
    Ok(())
}

#[test]
fn test_backup_r2() -> Result<()> {
    let num_nodes = 5;
    setup!(cluster, num_nodes);

    cluster.one(rand_cmd(), num_nodes, true)?;

    // put leader and one follower in a partition
    let leader1 = cluster.check_one_leader()?;
    cluster.disconnect((leader1 + 2) % num_nodes);
    cluster.disconnect((leader1 + 3) % num_nodes);
    cluster.disconnect((leader1 + 4) % num_nodes);

    // submit lots of command that won't commit
    for _ in 0..50 {
        cluster.exec_command(leader1, rand_cmd(), Some(Duration::from_millis(0)))?;
    }

    std::thread::sleep(max_election_timeout());

    // disable leader and the follower, put the partition on
    cluster.disconnect((leader1 + 0) % num_nodes);
    cluster.disconnect((leader1 + 1) % num_nodes);

    cluster.connect((leader1 + 2) % num_nodes);
    cluster.connect((leader1 + 3) % num_nodes);
    cluster.connect((leader1 + 4) % num_nodes);

    // lots of successful commands to new group.
    for _ in 0..50 {
        cluster.one(rand_cmd(), 3, true)?;
    }

    // now another partitioned leader and one follower
    let leader2 = cluster.check_one_leader()?;
    let mut other = (leader1 + 2) % num_nodes;
    if other == leader2 {
        other = (leader2 + 1) % num_nodes;
    }
    cluster.disconnect(other);

    // submit lots of command that won't commit
    for _ in 0..50 {
        cluster.exec_command(leader2, rand_cmd(), Some(Duration::from_millis(0)))?;
    }

    std::thread::sleep(max_election_timeout());

    // bring original leader back to life
    for i in 0..num_nodes {
        cluster.disconnect(i);
    }
    cluster.connect((leader1 + 0) % num_nodes);
    cluster.connect((leader1 + 1) % num_nodes);
    cluster.connect(other);

    // lots of successful commands to new group.
    for _ in 0..50 {
        cluster.one(rand_cmd(), 3, true)?;
    }

    // now everyone back to life
    for i in 0..num_nodes {
        cluster.connect(i);
    }

    cluster.one(rand_cmd(), num_nodes, true)?;

    teardown!(cluster);
    Ok(())
}

fn rand_cmd() -> Vec<u8> {
    let ans = thread_rng().gen_range(0..0xffffffffu32);
    ans.to_be_bytes().into()
}
