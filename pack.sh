rm -f dist/*
python setup.py bdist_wheel
twine upload dist/*
sudo pip install -U cos_migrate_tool_for_restore
