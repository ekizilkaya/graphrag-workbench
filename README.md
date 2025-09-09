# The Universe of Surveillance Capitalism (A Fork of GraphRAG Workbench)

This repository is a fork of Christopher Lyon's [GraphRAG Workbench](https://github.com/ChristopherLyon/graphrag-workbench). It contains specific modifications to enable the application to run on a Windows environment using Anaconda/Conda for Python management.

This version was used to create the data art piece "The Universe of Surveillance Capitalism," which visualizes the work of seven Technology and Human Rights fellows at Harvard Kennedy School's Carr-Ryan Center.

### Key Changes
*   Modified `app/api/corpus/index/stream/route.ts` and `lib/server/converters.ts` to use a bash login shell and absolute paths to the Conda Python executable, resolving execution errors on Windows.
*   Adjusted settings.yaml to utilize Mistral AI models as the LLM backend instead of the default OpenAI models.


### Data and Embeddings
To encourage reuse and scholarly adaptation, the visualization data and document embeddings are released under the MIT License. You can download the generated `.parquet` files from the following link:
**[Download the dataset from the Hugging Face Hub](https://huggingface.co/datasets/ekizilkaya/surveillance_capitalism)**

---
*(Original README content follows below)*
...