#[tokio::main]
async fn main() -> anyhow::Result<()> {
    upf::run().await
}
