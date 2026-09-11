# Snow Statistics engineering contract

- Business products, lightweight statistics and laboratory services are independently deployable and removable.
- Collection defaults off. Keep raw/chat/credential/IP/private feedback data out of this repository and public APIs.
- Never add a runtime or CI dependency from a business repository to this checkout, Kafka or Doris.
- Preserve event v1/summary v1 contracts; update explicit schema artifacts and golden metric tests for intentional changes.
- Use synthetic fixtures only. Label measured engines, inputs and resource conditions honestly; configuration is not integration evidence.
- Keep VM images, private keys, checkpoints, raw events, databases and environment credentials under ignored runtime paths.
- Respect the current resource limits in deploy/resources.json: 64 GiB project files, 35 GiB host free disk and 4 GiB host available RAM. The user authorized practical resource adjustments after the initial 60 GiB experiments; retained historical evidence keeps its original limits. Start profiles in stages.
- Never prune shared hosts globally. Generate a resource-specific retirement scope; switching off must not delete stored data.
- Run Python, browser and Java checks appropriate to the change. Maintain docs/status.md with verified and pending acceptance.
- Changes to Project_Snow follow its own AGENTS.md, including isolated worktrees and manual production promotion after candidate acceptance.
