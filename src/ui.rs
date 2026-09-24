use axum::http::header;
use axum::response::Html;

/// Lockdown headers for the embedded UI. `script-src 'unsafe-inline'`
/// is required by the single-file page (its <script> is inline); the
/// rest of the policy keeps it from loading anything off-origin.
const UI_HEADERS: [(axum::http::HeaderName, &str); 2] = [
    (
        header::CONTENT_SECURITY_POLICY,
        "default-src 'self'; script-src 'self' 'unsafe-inline'; img-src 'self' data:; connect-src 'self' ws: wss:; style-src 'self' 'unsafe-inline'",
    ),
    (header::X_CONTENT_TYPE_OPTIONS, "nosniff"),
];

/// Embedded single-file chat UI. Served unauthenticated (contains no
/// secrets); the page itself authenticates via the ws ticket flow.
pub async fn ui() -> impl axum::response::IntoResponse {
    (UI_HEADERS, Html(include_str!("ui.html")))
}

/// Installability manifest for the UI. The icon is referenced by path
/// (`/icon.svg`): Chromium's install pipeline fetches manifest icons
/// over HTTP and does not reliably accept `data:` URLs in `src`, so an
/// inline icon would silently break installability. No service worker
/// on purpose — current Chrome/Edge install apps without one, and
/// offline caching is out of scope for a daemon UI: the daemon itself
/// is the network dependency, so a cached shell has nothing to show.
pub async fn manifest() -> impl axum::response::IntoResponse {
    (
        UI_HEADERS,
        [(header::CONTENT_TYPE, "application/manifest+json")],
        // r## delimiter: the JSON contains `"#` (color values), which
        // would terminate a plain r#"…"# raw string.
        r##"{
  "name": "Damon",
  "description": "Damon agent core - daemon chat UI",
  "start_url": "/ui",
  "scope": "/",
  "display": "standalone",
  "background_color": "#0a0c10",
  "theme_color": "#0a0c10",
  "icons": [
    { "src": "/icon.svg", "sizes": "any", "type": "image/svg+xml", "purpose": "any" }
  ]
}
"##,
    )
}

/// Brand mark for the manifest and favicon: the UI logo (rounded
/// gradient square with the "d") as a standalone SVG.
pub async fn icon() -> impl axum::response::IntoResponse {
    (
        UI_HEADERS,
        [(header::CONTENT_TYPE, "image/svg+xml")],
        // r## delimiter: the SVG contains `"#` in url(#g), which would
        // terminate a plain r#"…"# raw string.
        r##"<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 64 64">
<defs>
<linearGradient id="g" x1="0" y1="0" x2="1" y2="1">
<stop offset="0" stop-color="#6ea8fe"/>
<stop offset="1" stop-color="#8957e5"/>
</linearGradient>
</defs>
<rect width="64" height="64" rx="15" fill="url(#g)"/>
<text x="32" y="44" font-family="-apple-system, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif" font-size="38" font-weight="800" fill="#ffffff" text-anchor="middle">d</text>
</svg>
"##,
    )
}
