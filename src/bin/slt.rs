use std::fs::File;
use std::io;
use std::io::Read;

use sboxdb::access::kv::Kv;
use sboxdb::slt::Slt;
use sboxdb::storage::memory::Memory;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = clap::command!()
        .name("slt")
        .about("SQL Logical Test evaluator")
        .arg(clap::Arg::new("file").short('f').long("file").help("The sqllogictest file to run"))
        .arg(
            clap::Arg::new("stdin")
                .long("stdin")
                .help("Read sqllogictest from stdin")
                .action(clap::ArgAction::SetTrue),
        )
        .arg(
            clap::Arg::new("check")
                .long("check")
                .help("Check if the give sqllogicaltest file is valid or not")
                .action(clap::ArgAction::SetTrue),
        )
        .get_matches();

    let stdin = args.get_one::<bool>("stdin").unwrap();
    let check = args.get_one::<bool>("check").unwrap();
    let script: Box<dyn Read> = if *stdin {
        Box::new(io::stdin())
    } else {
        let filename = args.get_one::<String>("file").unwrap();
        let file = File::open(filename)?;
        Box::new(file)
    };

    let slt = Slt::new(Kv::new(Memory::new()));
    if *check {
        slt.check_script(script)?;
        return Ok(());
    }
    let report = slt.evaluate_script("", script)?;
    println!("{}", report);
    Ok(())
}
