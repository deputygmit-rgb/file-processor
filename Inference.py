from transformers import AutoTokenizer, AutoModelForCausalLM
from peft import PeftModel
from transformers import pipeline

# --- Base model ---
tokenizer = AutoTokenizer.from_pretrained("distilgpt2")
model = AutoModelForCausalLM.from_pretrained("distilgpt2")

# --- Load LoRA adapter from checkpoint ---
lora_checkpoint = "./lora_railway_model/checkpoint-642"
model = PeftModel.from_pretrained(model, lora_checkpoint)

tokenizer.pad_token = tokenizer.eos_token

# --- CPU text generation pipeline ---
llm_pipe = pipeline(
    "text-generation",
    model=model,
    tokenizer=tokenizer,
    device=-1,   # CPU
    max_length=200
)

# --- Test inference ---
prompt = "Summarize failures related to point adjustment in Bilaspur division:"
output = llm_pipe(prompt)
print(output[0]["generated_text"])
