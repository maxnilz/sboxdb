use rustyline::error::ReadlineError;
use rustyline::history::DefaultHistory;
use rustyline::Editor;
use rustyline::Modifiers;
use sboxdb::client::Client;
use sboxdb::error::Error;
use sboxdb::error::Result;

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
        let editor = Editor::new()?;
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
        // Make sure multiline pastes are interpreted as normal inputs.
        self.editor.bind_sequence(
            rustyline::KeyEvent(rustyline::KeyCode::BracketedPasteStart, Modifiers::NONE),
            rustyline::Cmd::Noop,
        );
        println!("Connected to sboxdb. Enter \\help for instructions.",);
        while let Some(input) = self.prompt()? {
            match self.execute(&input).await {
                Ok(()) => {}
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

    async fn execute(&mut self, input: &str) -> Result<()> {
        if input.is_empty() {
            return Ok(());
        }
        if input.starts_with('\\') {
            return self.execute_command(input).await;
        }
        self.execute_query(input).await
    }

    /// Handles a REPL command (prefixed by \, e.g. \help)
    async fn execute_command(&mut self, input: &str) -> Result<()> {
        let mut input = input.split_ascii_whitespace();
        let command = input.next().ok_or_else(|| Error::parse("Expected command."))?;
        match command {
            "\\help" => println!(
                r#"
Enter a SQL statement terminated by a semicolon (;) to execute it and display the result.
The following commands are also available:

    \help        This help message
"#
            ),
            c => return Err(Error::parse(format!("Unknown command {}", c))),
        };
        Ok(())
    }

    async fn execute_query(&mut self, query: &str) -> Result<()> {
        match self.client.execute_query(query.into()).await {
            Ok(rs) => println!("{}", rs),
            Err(err) => return Err(err),
        };
        Ok(())
    }

    /// Prompts the user for input
    fn prompt(&mut self) -> Result<Option<String>> {
        let prompt = "sboxdb> ";
        match self.editor.readline(&prompt) {
            Ok(input) => {
                self.editor.add_history_entry(&input)?;
                Ok(Some(input.trim().to_string()))
            }
            Err(ReadlineError::Eof) | Err(ReadlineError::Interrupted) => Ok(None),
            Err(err) => Err(err.into()),
        }
    }
}
