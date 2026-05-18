use clap::{Args, Parser, Subcommand, ValueEnum};
use powdrr_lib::local_cli::{
    LocalParquetBuildRequest, LocalParquetQueryRequest, LocalQueryAnalysisRequest,
    LocalQueryLanguage, analyze_local_query, build_local_parquet_cache, query_local_parquet_cache,
};
use std::fs;
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "powdrr-cli")]
#[command(about = "Local CLI for querying parquet through powdrr's search stack")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Elastic(ElasticCommand),
}

#[derive(Args)]
struct ElasticCommand {
    #[command(subcommand)]
    command: ElasticSubcommand,
}

#[derive(Subcommand)]
enum ElasticSubcommand {
    Build(BuildArgs),
    Query(QueryArgs),
    Analyze(AnalyzeArgs),
}

#[derive(Args)]
struct BuildArgs {
    #[arg(long)]
    source: String,
    #[arg(long)]
    cache_dir: PathBuf,
    #[arg(long)]
    table: String,
    #[arg(long, default_value = "_id_seq_no")]
    doc_id_field: String,
    #[arg(long, default_value_t = false)]
    replace: bool,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, ValueEnum)]
enum QueryLanguageArg {
    EsJson,
}

#[derive(Args)]
struct QueryArgs {
    #[arg(long)]
    cache_dir: PathBuf,
    #[arg(long, value_enum, default_value_t = QueryLanguageArg::EsJson)]
    language: QueryLanguageArg,
    #[arg(long)]
    body: Option<String>,
    #[arg(long)]
    body_file: Option<PathBuf>,
    #[arg(long)]
    rest_total_hits_as_int: Option<bool>,
}

#[derive(Args)]
struct AnalyzeArgs {
    #[arg(long, value_enum, default_value_t = QueryLanguageArg::EsJson)]
    language: QueryLanguageArg,
    #[arg(long)]
    body: Option<String>,
    #[arg(long)]
    body_file: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Elastic(command) => match command.command {
            ElasticSubcommand::Build(args) => {
                let result = build_local_parquet_cache(&LocalParquetBuildRequest {
                    source: args.source,
                    cache_dir: args.cache_dir,
                    table_name: args.table,
                    doc_id_field: args.doc_id_field,
                    replace: args.replace,
                })
                .await?;
                println!("{}", serde_json::to_string_pretty(&result)?);
            }
            ElasticSubcommand::Query(args) => {
                let body = load_query_body(args.body, args.body_file)?;
                let response = query_local_parquet_cache(&LocalParquetQueryRequest {
                    cache_dir: args.cache_dir,
                    language: match args.language {
                        QueryLanguageArg::EsJson => LocalQueryLanguage::ElasticsearchJson,
                    },
                    body,
                    rest_total_hits_as_int: args.rest_total_hits_as_int,
                })
                .await?;
                println!("{}", response.body);
                if response.status_code >= 400 {
                    std::process::exit(1);
                }
            }
            ElasticSubcommand::Analyze(args) => {
                let body = load_query_body(args.body, args.body_file)?;
                let analysis = analyze_local_query(&LocalQueryAnalysisRequest {
                    language: match args.language {
                        QueryLanguageArg::EsJson => LocalQueryLanguage::ElasticsearchJson,
                    },
                    body,
                });
                println!("{}", serde_json::to_string_pretty(&analysis)?);
            }
        },
    }

    Ok(())
}

fn load_query_body(
    body: Option<String>,
    body_file: Option<PathBuf>,
) -> Result<String, Box<dyn std::error::Error>> {
    match (body, body_file) {
        (Some(body), None) => Ok(body),
        (None, Some(path)) => Ok(fs::read_to_string(path)?),
        (Some(_), Some(_)) => Err("Pass either --body or --body-file, not both".into()),
        (None, None) => Err("Pass one of --body or --body-file".into()),
    }
}
