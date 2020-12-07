from setuptools import setup, find_packages


setup(
    name="streamz_postgres",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "streamz @ git+https://github.com/python-streamz/streamz.git",
        "psycopg2-binary",
    ],
    extras_require={
        "dev": ["pytest", "flake8", "black"],
        "docs": ["sphinx", "sphinx_rtd_theme"],
    },
    entry_points={
        "streamz.sources": [
            "from_postgres_cdc = streamz_postgres.sources:from_postgres_cdc",
            "from_postgres_increment = "
            "streamz_postgres.sources:from_postgres_increment",
        ]
    },
)
