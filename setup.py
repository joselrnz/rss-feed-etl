from setuptools import setup, find_packages

setup(
    name="rss_feed_etl",
    version="1.0.0",
    description="RSS Feed ETL with SCD2 pattern for tracking changes",
    author="Your Name",
    author_email="your.email@example.com",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=[
        "feedparser>=6.0.0",
        "html2text>=2020.1.16",
        "pandas>=1.3.0",
        "gspread>=5.0.0",
        "oauth2client>=4.1.3",
        "beautifulsoup4>=4.9.3",
        "pytz>=2021.1",
        "python-dotenv>=0.19.0",
    ],
    python_requires=">=3.7",
    entry_points={
        "console_scripts": [
            "run-rss-etl=run_etl:main",
        ],
    },
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
    ],
)