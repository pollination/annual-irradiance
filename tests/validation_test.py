from pollination.annual_irradiance.entry import AnnualIrradianceEntryPoint
from queenbee.recipe.dag import DAG


def test_annual_irradiance():
    recipe = AnnualIrradianceEntryPoint().queenbee
    assert recipe.name == 'annual-irradiance-entry-point'
    assert isinstance(recipe, DAG)
