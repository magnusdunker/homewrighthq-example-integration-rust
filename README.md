# Eksempel-tæller til HomewrightHQ

En lille ekstern integration til [HomewrightHQ](https://github.com/magnusdunker/homewrighthq). Den er til at afprøve, hvordan integrationer installeres fra GitHub, og som udgangspunkt for at skrive sin egen.

Hver opsætning giver en enhed med:

- en sensor, **Antal**, der tæller én op med et fast interval
- en kontakt, **Tæller**, der sætter tællingen på pause
- en knap, **Nulstil**, der sætter tallet til 0 og sender hændelsen `example_counter_reset`

Desuden har den tjenesten `example_counter.set`, der sætter en tæller til en bestemt værdi, og et options-forløb, hvor intervallet kan ændres. Tallet gemmes i integrationens datamappe, så det overlever en genstart.

## Installation

I HomewrightHQ: **Indstillinger → Integrationer → Fra GitHub**. Indsæt

```
https://github.com/magnusdunker/homewrighthq-example-integration-rust
```

og tryk **Installér**. Tilføj derefter **Eksempel-tæller** under **Tilføj integration**.

## Sådan virker det

En ekstern integration er et selvstændigt program. HomewrightHQ starter det og taler med det over JSON-RPC på stdin og stdout. Protokollen er beskrevet i HomewrightHQ's [docs/custom-integrations.md](https://github.com/magnusdunker/homewrighthq/blob/main/docs/custom-integrations.md), så integrationen kan skrives i et hvilket som helst sprog.

Repoet indeholder:

- `thehome.json`: domæne, navn og protokolversion
- `sdk/`: crate'en `thehome-plugin`, der klarer protokollen i Rust
- `src/main.rs`: selve integrationen
- `.github/workflows/release.yml`: bygger en binær til hver platform og lægger dem på en release

## Lav din egen

1. Kopiér repoet og ret `domain` og `name` i `thehome.json`. Domænet må kun indeholde små bogstaver, tal og `_` og må ikke være en indbygget integrations domæne.
2. Ret `BIN` og `DOMAIN` øverst i `release.yml`, og navnet på binæren i `Cargo.toml`.
3. Implementér `Integration` fra `thehome-plugin`. Kun `setup_entry` er påkrævet.
4. Push et tag, f.eks. `git tag v0.1.0 && git push --tags`. Workflowet bygger til Windows, Linux (x86_64 og arm64) og macOS og opretter releasen.

HomewrightHQ henter altid den nyeste release. Binærerne skal hedde `<domæne>-<target>`, f.eks. `example_counter-aarch64-unknown-linux-musl`.

Prøv lokalt med `cargo build`. Programmet venter på beskeder fra HomewrightHQ på stdin, så det gør ingenting, når det startes alene.

## Licens

MIT
