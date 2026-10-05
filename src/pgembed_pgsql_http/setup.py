# Forces platform-specific wheel tags.
#
# The wheel ships a prebuilt http.so, so a py3-none-any tag would be wrong
# (pip could install a Linux .so on macOS). Reporting ext modules makes
# bdist_wheel emit cpXX-cpXX-<plat> instead.
from setuptools import setup
from setuptools.dist import Distribution


class BinaryDistribution(Distribution):
    def has_ext_modules(self):
        return True


setup(distclass=BinaryDistribution)
