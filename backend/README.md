# Backend Environment Setup

The FastAPI backend relies on `SUPABASE_URL` and `SUPABASE_SERVICE_ROLE_KEY` being configured at runtime. These are loaded via `python-dotenv` from a `.env` file in the `backend` directory (see `backend/api/main.py`).

Create a `.env` file beside this README and populate it with the Supabase credentials you intend to use locally:

```
SUPABASE_URL=https://<your-project-ref>.supabase.co
SUPABASE_SERVICE_ROLE_KEY=<your-service-role-key>
```

Keep that file out of source control (it is already ignored via `.gitignore`) and do **not** commit secrets. This document explains the required variables so you can manually create the `.env` file given that automated creation is not allowed in this workspace.


