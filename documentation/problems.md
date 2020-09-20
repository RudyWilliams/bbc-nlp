## Problems & Solutions

1. *Problem*: "No module named news_system" when running pytest. *Solution*: With this directory structure, "If you don't have a setup.py file and are relying on the fact that Python by default puts the current directory in sys.path to import your package, you can execute `python -m pytest` to execute the tests against a local copy directly, without using pip."