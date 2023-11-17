from flask_restx import fields


from ..extensions import api


user_model = api.models("User",{
    "id": fields.Integer,
    "username": fields.String,
    "email": fields.String
})