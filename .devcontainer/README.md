# Reproducible development workspace

This workspace provides the same credential-free starting path in GitHub Codespaces,
VS Code Dev Containers, and a local Docker-capable host. Open the repository in the
container, wait for setup to finish, then run:

```bash
./cookbooks/coding-agents/quickstart.sh
./cookbooks/coding-agents/dev-smoke.sh
./cookbooks/coding-agents/explore.sh
```

The first command independently audits the preserved Codex recovery evidence and prints
the first trustworthy recovery summary. The second runs that quickstart plus the focused
presentation, quickstart, and cookbook contract gates. Neither command invokes a model
provider, reads provider credentials, or starts a live Temporal service. The workspace does
not generate evidence.

## Pinned workspace

The image pins Go 1.25.12, uv 0.11.2, Python 3.12.12, Debian Bookworm, and both OCI image
indexes by SHA-256. It runs as the unprivileged `vscode` user, drops Linux capabilities,
sets `no-new-privileges`, and does not mount the Docker socket. The hermetic evidence audit
starts with an empty environment, so Codespaces secrets and host credentials are not
inherited into its child process.

The declared minimum is 2 CPU, 4 GB memory, and 16 GB storage. Those are onboarding
resource assumptions, not controlled compute and not performance evidence. The workspace
does not attempt to make laptop, Codespaces, or CI timing comparable.

## Boundaries

Container shutdown is explicit (`stopContainer`) and uses an init process for verified
shutdown and child reaping. The default path has no long-lived service or persistent
service volume. Live Temporal experiments, authenticated Claude/Codex experiments,
publication benchmarks, and evidence generation remain opt-in; use their own pinned
instructions, confined state roots, and teardown checks. Opening this workspace does not
authorize authenticated provider access or upgrade any preserved claim to current-source
evidence.

The explorer listens only on container loopback port 8080. Codespaces and Dev Containers
offer that port on demand; no service starts during setup.
