import importlib
packages = ['torch','pandas','numpy','joblib','sklearn','pyod','cassandra']
for pkg in packages:
    try:
        mod = importlib.import_module(pkg)
        print(f'OK: {pkg} ->', mod.__name__)
    except Exception as e:
        print(f'ERROR importing {pkg}:', type(e).__name__, str(e))
