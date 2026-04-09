from setuptools import setup, find_packages

setup(
    name='quantforce-core',
    version='0.1.0',
    packages=find_packages(),
    install_requires=[
        'psycopg2-binary',
        'pytz',
        'ib_insync',
        'groq',
        'requests',
        'pandas',
        'numpy',
    ],
    python_requires='>=3.10',
)
