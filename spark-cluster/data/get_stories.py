import re
import pathlib from Path

input_path = Path("collected_stories.txt")
output_dir = Path("stories")
output_dir.mkdir(exist_ok=True)

input_path.open("r", encoding="utf-8") as f
fulltxt = f.readlines()

current_title = None
buffer = []
prev_page_num = 1

for line in fulltxt:
    # Match lines like: newline(s), digit(s), space, anything-but-newline 
    match = re.match(r"^\s*(\d+)\s+(.+)", line.strip())
    # reached new page
    if match:
        if current_title and buffer:
            filename = current_title.lower().replace(" ", "-") + ".txt"
            with (output_dir / filename).open("a", encoding="utf-8") as out:
                out.write("".join(buffer))
            buffer = []

        # Start new section
        page_num, title = match.groups()
        assert page_num == (prev_page_num + 2)
        prev_page_num = page_num
        current_title = title
    # on the same page
    else:
        buffer.append(line)

# Write the final section
if current_title and buffer:
    filename = current_title.lower().replace(" ", "-") + ".txt"
    with (output_dir / filename).open("a", encoding="utf-8") as out:
        out.write("".join(buffer))







