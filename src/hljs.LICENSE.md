# src/hljs.min.js — provenance and license

`hljs.min.js` is a custom build of [highlight.js](https://highlightjs.org)
**v11.12.0** assembled from the official npm package with esbuild
(`--bundle --minify --format=iife`), registering these grammars:

bash, c, cpp, csharp, css, diff, dockerfile, go, ini, java, javascript,
json, markdown, makefile, python, rust, sql, typescript, xml, yaml
(plus their aliases: sh, shell, zsh, toml, html, yml, …).

Rebuild: `npm install highlight.js@11.12.0 esbuild`, bundle an entry that
imports `highlight.js/lib/core` plus the language modules above and calls
`registerLanguage` for each, then minify as an IIFE that assigns
`window.hljs`. Serve same-origin at `/hljs.js` (the UI CSP is `'self'`).

The UI treats a missing `hljs` global as "no highlighting" and falls back
to escaped plain code, so this file is an optional enhancement.

---

Highlight.js is licensed under the BSD-3-Clause License:

Copyright (c) 2006-2026, Josh Goebel <hello@joshgoebel.com> and
contributors.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:

1. Redistributions of source code must retain the above copyright notice,
   this list of conditions and the following disclaimer.

2. Redistributions in binary form must reproduce the above copyright
   notice, this list of conditions and the following disclaimer in the
   documentation and/or other materials provided with the distribution.

3. Neither the name of the copyright holder nor the names of its
   contributors may be used to endorse or promote products derived from
   this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS
IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR
CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
