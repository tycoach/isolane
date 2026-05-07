# GitHub Secrets Setup

The CI/CD pipelines require the following secrets configured in your
GitHub repository settings:

`Settings → Secrets and variables → Actions → New repository secret`

---

## Required secrets

| Secret | Description | Example |
|---|---|---|
| `TEST_ADMIN_EMAIL` | Email for the test admin user | `admin@isolane.dev` |
| `TEST_ADMIN_PASSWORD` | Password for the test admin user | `Isolane2026!` |

---

## Optional secrets (for CD pipeline)

These are only needed if you want the CD pipeline to push images to a
container registry and deploy to a server.

| Secret | Description |
|---|---|
| `GITHUB_TOKEN` | Auto-provided by GitHub Actions — no setup needed |
| `DEPLOY_SSH_KEY` | SSH private key for deployment server (if using SSH deploy) |
| `DEPLOY_HOST` | Hostname of deployment server |
| `DEPLOY_USER` | SSH user on deployment server |

---

## How to set secrets

```bash
# Using GitHub CLI
gh secret set TEST_ADMIN_EMAIL --body "admin@isolane.dev"
gh secret set TEST_ADMIN_PASSWORD --body "Isolane2026!"
```

Or via the GitHub UI:
1. Go to your repository on GitHub
2. Click **Settings** → **Secrets and variables** → **Actions**
3. Click **New repository secret**
4. Enter the name and value
5. Click **Add secret**

---

## Branch protection rules

Set up branch protection on `main` to require CI to pass before merge:

1. Go to **Settings** → **Branches** → **Add rule**
2. Branch name pattern: `main`
3. Enable:
   -  Require status checks to pass before merging
   -  Require branches to be up to date before merging
   - Status checks required:
     - `Python — lint + unit tests`
     - `Go — lint + build`
     - `Cross-tenant isolation tests`
     - `No secrets in code`
4. Enable:
   - Require linear history (optional but recommended)
   - Do not allow bypassing the above settings