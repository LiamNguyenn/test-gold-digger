## NOTICE WHEN USE TEST IN LOCAL
- Need to set environment variable `CHECKLY_API_KEY` to run test in local
- Need __init__.py file in every package that being ref in test
- Need to set `PYTHONPATH` to `extract/lambda/checkly_api_v2` to run test in local (Optional)
- CD inside tests folder to run test in local
- Clear pytest cache if you have any problem with test
- If you need to get log from print statement, use `pytest -s` to run test
- always remember to include ``` import sys sys.path.append("..") ``` in test
