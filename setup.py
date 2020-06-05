from setuptools import setup, find_packages
setup(
    name="FootballApp",
    version="0.1",
    packages=find_packages(),

    # Project uses reStructuredText, so ensure that the docutils get
    # installed or upgraded on the target machine
    install_requires=["docutils>=0.3"],
    package_data={
        # If any package contains *.txt or *.rst files, include them:
        "": ["*.txt", "*.rst"],
        "src": ["*.py"],
    },

    # metadata to display on PyPI
    author="Marion Hurteau",
    author_email="marion.hurteau1@gmail.com",
    description="Football App",
)
