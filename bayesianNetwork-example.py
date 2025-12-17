from pgmpy.models import DiscreteBayesianNetwork
from pgmpy.factors.discrete import TabularCPD
from pgmpy.inference import VariableElimination

# Define structure
model = DiscreteBayesianNetwork([('Rain', 'Sprinkler'), ('Rain', 'WetGrass'), ('Sprinkler', 'WetGrass')])

# Define probabilities
cpd_rain = TabularCPD('Rain', 2, [[0.7], [0.3]])
cpd_sprinkler = TabularCPD('Sprinkler', 2,
                           [[0.6, 0.99],
                            [0.4, 0.01]],
                           evidence=['Rain'],
                           evidence_card=[2])
cpd_wetgrass = TabularCPD('WetGrass', 2,
                          [[1, 0.1, 0.1, 0.01],
                           [0, 0.9, 0.9, 0.99]],
                          evidence=['Rain', 'Sprinkler'],
                          evidence_card=[2, 2])

model.add_cpds(cpd_rain, cpd_sprinkler, cpd_wetgrass)
infer = VariableElimination(model)

# Query
prob = infer.query(variables=['Rain'], evidence={'WetGrass': 1})
print(prob)
