"""Install Mapchete."""

from setuptools import setup, find_packages

version = "0.1"

setup(
    name="mapchete_hub",
    version=version,
    description="distributed mapchete processing",
    author="Joachim Ungar",
    author_email="joachim.ungar@eox.at",
    url="https://gitlab.eox.at/maps/mapchete_hub",
    license="MIT",
    packages=find_packages(),
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Topic :: Scientific/Engineering :: GIS",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
    ],
)
