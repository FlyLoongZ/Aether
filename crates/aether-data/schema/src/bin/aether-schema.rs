use std::path::PathBuf;

use aether_data_schema::dialect::postgres;
use aether_data_schema::{check_generated_dir, generate_loaded_to_dir, load_schema_sources};
use clap::{Parser, Subcommand, ValueEnum};

#[derive(Debug, Parser)]
#[command(name = "aether-schema")]
#[command(about = "Generate SQL from Aether logical schema definitions")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    Generate {
        #[arg(long, default_value = "crates/aether-data/runtime/schema/logical")]
        schema_dir: PathBuf,
        #[arg(long, default_value = "crates/aether-data/runtime/schema/generated")]
        output_dir: PathBuf,
    },
    Check {
        #[arg(long, default_value = "crates/aether-data/runtime/schema/logical")]
        schema_dir: PathBuf,
        #[arg(long, default_value = "crates/aether-data/runtime/schema/generated")]
        output_dir: PathBuf,
        #[arg(long = "require-tables-from")]
        require_tables_from: Vec<PathBuf>,
    },
    Print {
        #[arg(long, default_value = "crates/aether-data/runtime/schema/logical")]
        schema_dir: PathBuf,
        #[arg(long)]
        driver: Driver,
    },
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum Driver {
    Postgres,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    match cli.command {
        Command::Generate {
            schema_dir,
            output_dir,
        } => {
            let loaded = load_schema_sources(schema_dir)?;
            generate_loaded_to_dir(&loaded, output_dir)?;
        }
        Command::Check {
            schema_dir,
            output_dir,
            require_tables_from,
        } => {
            let loaded = load_schema_sources(schema_dir)?;
            check_generated_dir(&loaded, output_dir)?;
            // The baseline is a historical starting point: migrations retire
            // some of its definitions, so the coverage check must ignore those
            // instead of forcing the baseline to be rewritten.
            aether_data_schema::check_required_tables_excluding_retired(
                &loaded.schema,
                &require_tables_from,
            )?;
        }
        Command::Print { schema_dir, driver } => {
            let schema = load_schema_sources(schema_dir)?.schema;
            let output = match driver {
                Driver::Postgres => postgres::emit_schema(&schema),
            };
            print!("{output}");
        }
    }
    Ok(())
}
