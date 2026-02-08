from backend.api.main import app as fastapi_app


async def handler(request):
    return await fastapi_app(request.scope, request.receive, request.send)

