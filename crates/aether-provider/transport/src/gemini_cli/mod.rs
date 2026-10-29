mod profile;
pub use profile::{
    gemini_cli_client_user_agent, gemini_cli_client_version, set_gemini_cli_client_version,
};

pub const GEMINI_CLI_V1INTERNAL_ENVELOPE_NAME: &str = "gemini_cli:v1internal";
