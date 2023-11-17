from ...extensions import api
from flask_restx import fields

auth_model_response = api.model("user", {
    "id": fields.Integer(readonly=True, description="id"),
    "username": fields.String(readonly=True, description="username"),
    "email": fields.String(readonly=True, description="email")
})
