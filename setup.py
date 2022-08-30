from setuptools import setup

about = {}
with open("wakaq/__about__.py") as f:
    exec(f.read(), about)

install_requires = [x.strip() for x in open("requirements.txt").readlines()]

setup(
    name=about["__title__"],
    version=about["__version__"],
    license=about["__license__"],
    description=about["__description__"],
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    author=about["__author__"],
    author_email=about["__author_email__"],
    url=about["__url__"],
    packages=["wakaq"],
    package_dir={"wakaq": "wakaq"},
    python_requires=">= 3.7",
    include_package_data=True,
    platforms="any",
    install_requires=install_requires,
    entry_points={
        "console_scripts": [
            "wakaq-worker = wakaq.cli:worker",
            "wakaq-scheduler = wakaq.cli:scheduler",
            "wakaq-info = wakaq.cli:info",
            "wakaq-purge = wakaq.cli:purge",
        ],
    },
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: BSD License",
        "Natural Language :: English",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3 :: Only",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: System :: Distributed Computing",
        "Topic :: Software Development :: Object Brokering",
        "Operating System :: OS Independent",
    ],
)
