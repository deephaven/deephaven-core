# Deephaven Design System

A brand-aligned design system for **Deephaven** — the real-time, update-driven data engine and analytics platform. Deephaven combines a high-performance query engine with an interactive, IDE-style web workspace where users write queries (Python / Groovy / SQL) and watch tables, charts, and aggregations update live as data streams in.

This system captures Deephaven's brand foundations (color, type, logo) and provides reusable UI primitives plus a UI-kit recreation of the product's IDE/console, so designers and agents can produce on-brand interfaces, mocks, decks, and prototypes.

## Sources provided

- **Logo mark** — `uploads/Logo.png` (2×3 pixel-grid mark: indigo, cyan, red, yellow)
- **Wordmarks** — `uploads/Wordmark-OnLight.png`, `Wordmark-OnDark.png`, `Wordmark-OneColor-OnLight.png`, `Wordmark-OneColor-OnDark.png`
- **Color reference** — `uploads/deep colors.png` (8 named ramps × 11 steps)
- **Fonts** (specified by brand owner): **Inter** (UI/display) and **Fira Mono** (code/data)

No product codebase or Figma file was provided. Component APIs below are an authored standard set built to Deephaven's brand foundations, and the UI kit is a visual recreation of Deephaven's IDE informed by the public product — **not** copied from source. Verify against the real product before production use.

---

## Content fundamentals — how Deephaven writes

- **Voice:** technical, precise, confident. Speaks to engineers and quants who value performance and correctness over marketing gloss.
- **Person:** addresses the user as **"you"**; describes the product in plain third person ("Deephaven updates tables as data arrives").
- **Casing:** **Sentence case** everywhere — buttons, headings, menu items ("Run query", "New table", not "Run Query"). Product/feature names keep their capitalization (Deephaven, Core, Enterprise).
- **Tone specifics:** verbs are action-first and short ("Run", "Restart worker", "Download CSV"). Numbers and units are exact and monospaced (`1,240,908 rows`, `41ms`, `227.14`). Prefer concrete data terms — table, query, worker, tick, stream, aggregation.
- **Emoji:** not used in product UI. Avoid.
- **Vibe:** a fast, dense, keyboard-friendly workbench. Copy respects the reader's expertise; no hand-holding, no exclamation marks.

Examples: _"Connected to worker · Python 3.11"_, _"trades → table (streaming)"_, _"This permanently removes trades. This can't be undone."_

---

## Visual foundations

- **Color:** eight semantic ramps, each 11 steps where **100 = darkest, 1100 = lightest**. Primary is a deep indigo/blue; secondary a bright cyan; then negative (red/magenta), positive (green), warn (amber), info (purple), plus navy `fg` and cool-gray `bg` neutrals. The four logo accents (indigo `#3d51b3`, cyan `#4dccfa`, red `#f33666`, yellow `#fdd041`) are reserved for the mark and rare highlight moments — not general UI fills.
- **Dark-first:** Deephaven's product runs dark by default (navy `fg` surfaces, cyan links, bright semantic ticks). The system ships both a light surface set (default `:root`) and a dark set (`[data-theme="dark"]`). The UI kit uses dark.
- **Type:** **Inter** for everything UI and display, with tight tracking (`-0.02em`) on large headings. **Fira Mono** for code, table cells, IDs, and any numeric/tabular value — monospace is a core brand signal here. Base UI size is a dense **14px**.
- **Spacing:** 4px base grid, tight by default (dense data tooling). Generous whitespace is reserved for marketing surfaces, not the app.
- **Corners:** crisp and small — 2/4/6px radii. Data tools feel precise, not soft/rounded. Pills only for switches and avatars.
- **Borders:** hairline 1px dividers (`--dh-border`) carry most of the structure; the UI is grid-and-line driven, not shadow-heavy.
- **Shadows:** subtle, cool-toned (navy-tinted). Used only for truly floating surfaces (dialogs, popovers, tooltips). Panels use borders, not elevation.
- **Backgrounds:** flat solid fills. **No gradients, no textures, no illustration washes.** The visual interest comes from live data, syntax color, and the pixel-grid mark.
- **Animation:** quick and functional — 90–240ms, ease `cubic-bezier(0.2,0,0.2,1)`. No bounce, no spring. The signature motion is the **tick flash**: a cell briefly tints green/red on value change, then fades. Fades and color transitions only.
- **Hover states:** subtle background fill (`--dh-surface-hover`); secondary buttons shift border to focus-blue. **Press:** darker fill (no scale/shrink). **Focus:** 2px brand-blue ring.
- **Transparency/blur:** used sparingly — dialog scrim is navy at ~45% with a faint 1px blur. Semantic "soft" badges use `color-mix` tints.
- **Cards/panels:** flat surface, 1px border, 6px radius, optional subtle shadow only when floating. Header row divided by a hairline.

---

## Iconography

- **Style:** clean line icons, ~2px stroke, rounded caps — matching modern data-tooling UIs.
- **Substitution (flagged):** no product icon set was provided, so this system uses **[Lucide](https://lucide.dev)** via CDN as the closest open match. Load once per page: `<script src="https://unpkg.com/lucide@latest/dist/umd/lucide.min.js"></script>`. The `Icon` and `IconButton` components wrap it. **→ Replace with Deephaven's own SVG sprite for production.**
- **Emoji:** never used as icons.
- **Unicode:** middle-dot `·` is used as a metadata separator in status strings; otherwise avoid decorative glyphs.
- **Logo:** the pixel-grid mark is a raster PNG asset (`assets/`) — never redraw it in code. Use the provided wordmarks for lockups.

---

## Fonts note (action needed)

Inter and Fira Mono are loaded from **Google Fonts** (`tokens/fonts.css`) via `@import`, so no binaries are bundled (`@font-face` count is 0). If you need self-hosted webfont files for offline/production use, drop the `.woff2` files in `assets/fonts/` and replace the `@import` with `@font-face` rules. **Please confirm** whether CDN loading is acceptable or provide font binaries.

---

## Index / manifest

**Root**

- `styles.css` — global entry point (consumers link this). `@import`s everything below.
- `tokens/` — `fonts.css`, `colors.css`, `typography.css`, `spacing.css`, `effects.css`
- `assets/` — logo mark, four wordmarks, color reference PNG
- `guidelines/` — foundation specimen cards (Colors, Type, Spacing, Brand)
- `components/` — reusable UI primitives (below)
- `ui_kits/console/` — Deephaven IDE recreation (`index.html` + JSX)
- `SKILL.md` — Agent Skills manifest

**Components** (namespace `window.DeephavenDesignSystem_*` when consumed via the bundle)

- `controls/` — **Button**, **IconButton**
- `forms/` — **Input**, **Select**, **Checkbox**, **Radio**, **Switch**
- `display/` — **Card**, **Badge**, **Tag**, **Icon**
- `overlay/` — **Dialog**, **Tooltip**
- `navigation/` — **Tabs**

**Intentional additions:** `Icon` — a thin wrapper over the Lucide set, added because a glyph system is required and no product sprite was supplied.

**UI kits**

- `ui_kits/console/` — the real-time IDE: top bar + wordmark, panel explorer, code console with syntax-lit query and run log, and a live streaming data grid with tick-flash animation.
