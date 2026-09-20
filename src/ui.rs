use axum::response::Html;

/// Embedded single-file chat UI. Served unauthenticated (contains no
/// secrets); the page itself authenticates via the ws ticket flow.
pub async fn ui() -> Html<&'static str> {
    Html(include_str!("ui.html"))
}
