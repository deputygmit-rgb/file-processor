# train_lora_cpu_wandb.py
import json
import wandb
from datasets import Dataset
from transformers import (
    AutoTokenizer, AutoModelForCausalLM, Trainer, TrainingArguments,
    DataCollatorForLanguageModeling
)
from peft import LoraConfig, get_peft_model, TaskType

# 1️⃣ Initialize wandb
wandb.init(project="railway_lora_training", name="lora_distilgpt2_cpu")

# 2️⃣ Load JSON dataset
with open("xlsx_data.json", "r", encoding="utf-8") as f:
    raw_json = json.load(f)

headers = raw_json["data"][0]
rows = raw_json["data"][1:]

records = [{headers[i]: row[i] for i in range(len(headers))} for row in rows]

# Normalize records
def normalize_record(r):
    return {
        "sr_no": r.get("Sr. No",""),
        "icms_id": r.get("ICMS Id",""),
        "smms_remark": r.get("SMMS Remark",""),
        "zone": r.get("Zone",""),
        "division": r.get("Division",""),
        "section": r.get("Section",""),
        "gear": r.get("Mapped S&T Gear",""),
        "block_section": r.get("Block Section",""),
        "icms_remark": r.get("ICMS Remark",""),
        "start_time": r.get("start time",""),
        "failure_start_date": r.get("Failure Start Date",""),
        "end_time": r.get("End time",""),
        "failure_end_date": r.get("Failure End Date",""),
    }

normalized_records = [normalize_record(r) for r in records]

# Combine fields into a single string
def combine_fields(r):
    text = f"""
Sr No: {r['sr_no']}, ICMS Id: {r['icms_id']}, SMMS Remark: {r['smms_remark']},
Zone: {r['zone']}, Division: {r['division']}, Section: {r['section']},
Gear: {r['gear']}, Block Section: {r['block_section']}, ICMS Remark: {r['icms_remark']},
Start Time: {r['start_time']}, Failure Start Date: {r['failure_start_date']},
End Time: {r['end_time']}, Failure End Date: {r['failure_end_date']}
"""
    return {"text": text.strip()}

dataset_records = [combine_fields(r) for r in normalized_records]
dataset = Dataset.from_list(dataset_records)

# 3️⃣ Load tokenizer and model
base_model_name = "distilgpt2"
tokenizer = AutoTokenizer.from_pretrained(base_model_name)
tokenizer.pad_token = tokenizer.eos_token

model = AutoModelForCausalLM.from_pretrained(base_model_name)

# 4️⃣ LoRA adapter
lora_config = LoraConfig(
    r=8,
    lora_alpha=16,
    target_modules=["c_attn"],
    lora_dropout=0.1,
    bias="none",
    task_type=TaskType.CAUSAL_LM
)
model = get_peft_model(model, lora_config)

# 5️⃣ Tokenization
def tokenize_fn(batch):
    return tokenizer(batch["text"], truncation=True, padding="max_length", max_length=256)

tokenized_ds = dataset.map(tokenize_fn, batched=True)

data_collator = DataCollatorForLanguageModeling(tokenizer, mlm=False)

# 6️⃣ Training arguments with wandb
training_args = TrainingArguments(
    output_dir="./lora_railway_model",
    per_device_train_batch_size=2,
    gradient_accumulation_steps=8,
    num_train_epochs=3,
    logging_steps=5,
    save_steps=50,
    save_total_limit=2,
    learning_rate=5e-4,
    fp16=False,
    report_to="wandb",  # Enable wandb logging
    run_name="lora_distilgpt2_cpu",
)

# 7️⃣ Trainer
trainer = Trainer(
    model=model,
    args=training_args,
    train_dataset=tokenized_ds,
    tokenizer=tokenizer,
    data_collator=data_collator
)

# 8️⃣ Train
trainer.train()

# 9️⃣ Save LoRA adapter
model.save_pretrained("./lora_railway_adapter")
print("✅ LoRA adapter training complete!")

# 1️⃣0️⃣ Finish wandb run
wandb.finish()
