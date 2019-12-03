from tests import *

if __name__ == "__main__":
    import dotenv
    import unittest
    from django.conf import settings
    try:
        dotenv.read_dotenv()
    except AttributeError:
        dotenv.load_dotenv()
    
    settings.configure()
    unittest.main()


