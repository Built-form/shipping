# Archive: hand-applied SQL (not executed)

Schema changes now live in [`../migrate/`](../migrate/) and are applied by
`tools/migrate.js`, which `deploy.sh` runs before every deploy. See the
"Schema changes" section of the repo README.

The files here are the history from before that. Nothing runs them. Their
effect on every table this app owns is captured in
`../migrate/2026-09-21_00_baseline.sql` (generated from production), and the
still-pending ones became the `2026-09-21_0N_*` files next to it.

Still hand-applied from here, as admin, because they are cross-database
`DEFINER` objects over the `jfpro` schema rather than app tables:

- `2026-06-26_product_carton_sizes_view.sql` (view `product_carton_sizes`)
- `2026-06-30_suppliers_views.sql` (views `suppliers`, `supplier_emails`)
- `2026-06-30_jfpro_portal_code_trigger.sql` (trigger on `jfpro.suppliers`)

`node tools/migrate.js --status` reports any of those views missing.

Several headers below say "created at runtime by ...": that is no longer true,
the Lambdas run no DDL.
