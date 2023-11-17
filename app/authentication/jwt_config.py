from datetime import timedelta


class JWTConfig:
    SECRET_KEY = '78214125442A472D4B6150645367566B59703373367639792F423F4528482B4D'  # Replace with your secret key
    JWT_ACCESS_TOKEN_EXPIRES = timedelta(days=90)  # Set the expiration time to 3 months (90 days)
    # JWT_REFRESH_TOKEN_EXPIRES = False  # Disable refresh token expiration
    # JWT_BLACKLIST_ENABLED = True
    # JWT_BLACKLIST_TOKEN_CHECKS = ['access', 'refresh']
    JWT_TOKEN_LOCATION = ['headers']
    JWT_ALGORITHM = 'HS256'
