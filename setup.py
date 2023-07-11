"""Setup pipelines."""
import os

import setuptools

package_dir = os.path.dirname(os.path.realpath(__file__))
requirement_path = package_dir + "/requirements.txt"
with open(requirement_path, "r", encoding="UTF-8") as f:
    requirements = f.read().splitlines()

setuptools.setup(
    name="test-glue-job",
    version="0.1.1",
    description="Business screening",
    url="https://github.com/itsbigspark/test_glue",
    maintainer="Business screening",
    license="Proprietary",
    include_package_data=True,
    long_description="Business screening",
    packages=setuptools.find_namespace_packages(
        include=[
            "src.utils",
        ]
    ),
    package_data={
        "src": ["config.ini"],
    },
    install_requires=requirements,
)
