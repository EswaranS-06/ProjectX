import importlib
packages = ['torch','joblib','pandas','numpy','sklearn','pyod','scapy','prettytable','tensorflow','cassandra']
for pkg in packages:
    try:
        importlib.import_module(pkg)
        print('OK:',pkg)
    except Exception as e:
        print('MISSING:',pkg,'->', type(e).__name__, e)
