use rustyline::error::ReadlineError;
use rustyline::history::DefaultHistory;
use rustyline::Editor;
use sboxdb::client::Client;
use sboxdb::error::Error;
use sboxdb::error::Result;
use sboxdb::parse_err;
use sboxdb::sql::format;

#[tokio::main]
async fn main() -> Result<()> {
    let args = clap::command!()
        .name("sboxsql")
        .about("SandboxDB client")
        .arg(
            clap::Arg::new("addr")
                .short('a')
                .long("addr")
                .help("Address to connect to in format host:port")
                .default_value("localhost:8911"),
        )
        .get_matches();
    let mut sboxsql = SboxSQL::try_new(args.get_one::<String>("addr").unwrap()).await?;
    sboxsql.run().await
}

/// The sbox SQL REPL(Read-Eval-Print Loop) client.
struct SboxSQL {
    client: Client,

    editor: Editor<(), DefaultHistory>,
    history_path: Option<std::path::PathBuf>,
}

impl SboxSQL {
    async fn try_new(addr: &str) -> Result<Self> {
        let client = Client::try_new(addr).await?;
        let editor = Editor::<(), DefaultHistory>::new()?;
        let history_path = std::env::var_os("HOME")
            .map(|home| std::path::Path::new(&home).join(".sboxsql.history"));
        Ok(Self { client, editor, history_path })
    }

    async fn run(&mut self) -> Result<()> {
        if let Some(path) = &self.history_path {
            match self.editor.load_history(path) {
                Ok(_) => {}
                Err(ReadlineError::Io(ref err)) if err.kind() == std::io::ErrorKind::NotFound => {}
                Err(err) => return Err(err.into()),
            };
        }
        println!("Welcome to sboxdb shell. Type \\help to learn more.",);
        while let Some(input) = self.prompt()? {
            match self.execute(&input).await {
                Ok(action) => match action {
                    Action::Continue => {}
                    Action::Stop => break,
                },
                Err(err) => match err {
                    Error::Internal(_) => return Err(err),
                    err => println!("Error: {}", err),
                },
            }
        }
        if let Some(path) = &self.history_path {
            self.editor.save_history(path)?;
        }
        Ok(())
    }

    async fn execute(&mut self, input: &str) -> Result<Action> {
        if input.is_empty() {
            return Ok(Action::Continue);
        }
        if input.starts_with('\\') {
            return self.execute_command(input).await;
        }
        self.execute_query(input).await
    }

    /// Handles a REPL command (prefixed by \, e.g. \help)
    async fn execute_command(&mut self, input: &str) -> Result<Action> {
        let mut input = input.split_ascii_whitespace();
        let command = input.next().ok_or_else(|| parse_err!("Expected command."))?;
        match command {
            "\\help" => {
                println!("{}", self.help_msg());
                Ok(Action::Continue)
            }
            "\\q" => {
                println!("Bye!");
                Ok(Action::Stop)
            }
            "\\d" => {
                if let Some(table) = input.next() {
                    return self.execute_query(format!("SHOW CREATE TABLE {}", table)).await;
                }
                self.execute_query("SHOW TABLES").await
            }
            "\\tpcc" => self.execute_query("CREATE DATASET tpcc").await,
            _ => Err(parse_err!("Unknown command {}", command)),
        }
    }

    async fn execute_query(&mut self, query: impl Into<String>) -> Result<Action> {
        match self.client.execute_query(query.into()).await {
            Ok(rs) => {
                if rs.is_empty() {
                    println!("done");
                    return Ok(Action::Continue);
                }
                println!("{}", rs);
                Ok(Action::Continue)
            }
            Err(err) => Err(err),
        }
    }
    fn help_msg(&self) -> String {
        format::dedent(
            r#"
                Enter a SQL statement terminated by a semicolon (;) to execute it and display the result.
                The following commands are also available:
                     \help        This help message
                    \q           Quite the shell
                    \d           List tables
                    \d NAME      Describe table
                    \tpcc        Load tpcc dataset
            "#,
            true,
        )
    }

    /// Prompts the user for input
    fn prompt(&mut self) -> Result<Option<String>> {
        let mut output = String::new();
        loop {
            let prompt = if output.is_empty() { "sboxdb> " } else { "....>>> " };
            match self.editor.readline(&prompt) {
                Ok(input) => {
                    output.push_str(&input.trim());
                    if input.starts_with("\\") || output.trim().ends_with(";") {
                        break;
                    }
                }
                Err(ReadlineError::Eof) | Err(ReadlineError::Interrupted) => {
                    return Ok(None);
                }
                Err(err) => return Err(err.into()),
            }
        }
        self.editor.add_history_entry(&output)?;
        Ok(Some(output))
    }
}

pub enum Action {
    Continue,
    Stop,
}
