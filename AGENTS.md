# Snow Statistics engineering contract

- Business products, lightweight statistics and laboratory services are independently deployable and removable.
- Collection defaults off. Keep raw/chat/credential/IP/private feedback data out of this repository and public APIs.
- Never add a runtime or CI dependency from a business repository to this checkout, Kafka or Doris.
- Preserve event v1/summary v1 contracts; update explicit schema artifacts and golden metric tests for intentional changes.
- Use synthetic fixtures only. Label measured engines, inputs and resource conditions honestly; configuration is not integration evidence.
- Keep VM images, private keys, checkpoints, raw events, databases and environment credentials under ignored runtime paths.
- Respect the 60 GiB initial physical project budget and 35 GiB host free disk reserve. Start profiles in stages and check free RAM.
- Never prune shared hosts globally. Generate a resource-specific retirement scope; switching off must not delete stored data.
- Run Python, browser and Java checks appropriate to the change. Maintain docs/status.md with verified and pending acceptance.
- Changes to Project_Snow follow its own AGENTS.md, including isolated worktrees and manual production promotion after candidate acceptance.
