.PHONY: dev test

# Local dev run: the real server against the real broker, HA and printer —
# the gate before any beta tag (see AGENTS.md §Branches and PRs). The first
# run starts with an empty instance: add the printer in the UI, set the
# broker URL on the Integrations page, enable the bridge under Features.
# State lives in .dev-data (gitignored) so the setup survives restarts.
dev:
	DATA_DIR=$(CURDIR)/.dev-data venv/bin/python server.py

test:
	venv/bin/pytest
