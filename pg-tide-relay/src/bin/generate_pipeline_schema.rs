use std::fs;
use std::io;

fn main() -> io::Result<()> {
    let schema = pg_tide_relay::config::schema_support::pipeline_schema();
    let text = serde_json::to_string_pretty(&schema).map_err(io::Error::other)?;
    fs::create_dir_all("schemas")?;
    fs::write(
        "schemas/pipeline-config-v1.schema.json",
        format!("{text}\n"),
    )?;
    Ok(())
}
