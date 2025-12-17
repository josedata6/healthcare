# import nltk

# from nltk.tokenize import sent_tokenize

# def tokenize_sentences(text):
#       sentences = sent_tokenize(text)
#       return sentences

# text = “The patient presents with acute exacerbation of chronic obstructive pulmonary disease (COPD). Complains of increased shortness of breath, productive cough with green sputum, and wheezing. Vitals on arrival: BP 140/85, HR 95, RR 22, SpO2 88% on room air. Started on nebulized albuterol and ipratropium, as well as oral prednisone.”

# # Tokenize sentences
# sentences = tokenize_sentences(text)

# # Print tokenized sentences
# for i, sentence in enumerate(sentences):
#     print(f"Sentence {i+1}: {sentence}")




# import nltk
# from nltk.tokenize import sent_tokenize

# def tokenize_sentences(text):
#     sentences = sent_tokenize(text)
#     return sentences

# text = "The patient presents with acute exacerbation of chronic obstructive pulmonary disease (COPD). Complains of increased shortness of breath, productive cough with green sputum, and wheezing. Vitals on arrival: BP 140/85, HR 95, RR 22, SpO2 88% on room air. Started on nebulized albuterol and ipratropium, as well as oral prednisone."

# # Tokenize sentences
# sentences = tokenize_sentences(text)

# # Print tokenized sentences
# for i, sentence in enumerate(sentences):
#     print(f"Sentence {i+1}: {sentence}")


from nltk.stem import WordNetLemmatizer


from nltk.stem import WordNetLemmatizer
from nltk.tokenize import word_tokenize

text = "Very orderly and methodical he looked, with a hand on each knee, and a loud watch ticking a sonorous sermon under his flapped newly bought waist-coat, as though it pitted its gravity and longevity against the levity and evanescence of the brisk fire."

# tokenise text
tokens = word_tokenize(text)

wordnet_lemmatizer = WordNetLemmatizer()
lemmatized = [wordnet_lemmatizer.lemmatize(token) for token in tokens]
print(lemmatized)
