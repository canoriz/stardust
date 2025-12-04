use stardust::app;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    app::main().await
}
