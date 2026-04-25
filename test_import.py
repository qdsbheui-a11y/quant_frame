import sys
print("Python 路径：")
for path in sys.path:
    print(f"  {path}")

print("\n尝试导入 my_bt_lab...")
try:
    import my_bt_lab
    print("✅ 导入成功！")
except ImportError as e:
    print(f"❌ 导入失败: {e}")
    print("\n请确保：")
    print("1. 在项目根目录运行脚本")
    print("2. 或将 my_bt_lab_institutional_starter 添加到 PYTHONPATH")
