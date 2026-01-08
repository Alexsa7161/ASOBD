import os
import subprocess
import sys

# ===============================  
# Настройки
# ===============================
PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))
RESULT_DIR = os.path.join(PROJECT_DIR, "result")
os.makedirs(RESULT_DIR, exist_ok=True)

REQUIREMENTS_FILE = os.path.join(PROJECT_DIR, "requirements.txt")
COVERAGE_HTML = os.path.join(RESULT_DIR, "html")
COVERAGE_XML = os.path.join(RESULT_DIR, "coverage.xml")

# ===============================
# Установка зависимостей
# ===============================
print("Устанавливаем зависимости из requirements.txt...")
subprocess.run([sys.executable, "-m", "pip", "install", "--upgrade", "pip"], check=True)
subprocess.run([sys.executable, "-m", "pip", "install", "-r", REQUIREMENTS_FILE], check=True)

# ===============================
# Собираем список тестов
# ===============================
test_files = []
exclude_dirs = {"airflow", "__pycache__", "venv", ".venv"}
for root, dirs, files in os.walk(PROJECT_DIR):
    dirs[:] = [d for d in dirs if d not in exclude_dirs]
    for f in files:
        if f.startswith("test_") and f.endswith(".py"):
            test_files.append(os.path.join(root, f))

print(f"Найдено тестов: {len(test_files)}")

# ===============================
# Запуск ЮНИТ-тестов с coverage
# ===============================
pytest_cmd = [
    sys.executable, "-m", "pytest",
    "--cov",
    "--cov-report=term",
    f"--cov-report=html:{COVERAGE_HTML}",
    f"--cov-report=xml:{COVERAGE_XML}",
    "-m", "not integration"
] + test_files

result = subprocess.run(pytest_cmd)
if result.returncode != 0:
    print("Есть ошибки ЮНИТ-тестов")
    sys.exit(result.returncode)

print("ЮНИТ-тесты выполнены успешно")

INTEGRATION_DIR = os.path.join(RESULT_DIR, "integration")
os.makedirs(INTEGRATION_DIR, exist_ok=True)

print("\nЗапускаем интеграционные тесты...")
integration_cmd = [
    sys.executable, "-m", "pytest",
    "-m", "integration",                    
    "-v",                                   
    "--tb=short",                           
    f"--junitxml={INTEGRATION_DIR}/junit.xml",  
    f"--html={INTEGRATION_DIR}/report.html",   
    "--self-contained-html",             
    "tests/test_integration_all_systems.py"  
]

result_int = subprocess.run(integration_cmd)
if result_int.returncode != 0:
    print("Есть ошибки интеграционных тестов")
    sys.exit(result_int.returncode)

print("интеграционные тесты выполнены успешно")
print(f"JUnit XML: {INTEGRATION_DIR}/junit.xml")
print(f"HTML отчёт: {INTEGRATION_DIR}/report.html")
