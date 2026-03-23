
def simulate_extraction(inner_text):
    label_map = {
        'service':                            'service',
        'meal type':                          'meal_type',
        'price per person':                   'price_per_person',
        'wait time':                          'wait_time',
        'cleanliness':                        'cleanliness',
        'noise level':                        'noise_level',
        'parking space':                      'parking_space',
        'kids menu':                          'kids_menu',
        'reservations':                       'reservations',
        'accessibility':                      'accessibility',
        'amenities':                          'amenities',
        'recommended dishes':                 'recommended_dishes',
        'recommendation for vegetarians':     'vegetarian_recommendation',
        'vegetarian offerings':               'vegetarian_offerings',
        'getting there':                      'getting_there',
        'planning':                           'planning',
        'group size':                         'group_size', 
        'wheelchair accessibility':           'wheelchair_accessibility',
    }
    dining_modes = {"dine in", "take out", "delivery", "takeaway", "drive-through", "curbside pickup"}

    def strip_ellipsis(s):
        import re
        return re.sub(r'[\u2026]|\.\.\.[\.]*', '', s).strip()

    seen_lines = set()
    lines = []
    for s in inner_text.split('\n'):
        s = s.strip()
        if not s or s in seen_lines:
            continue
        seen_lines.add(s)
        lines.append(s)

    attrs = {}
    consumed = set()

    # Pattern A
    import re
    sub_rating_re = re.compile(r'^(.+?):\s*([1-5])$')
    for idx, line in enumerate(lines):
        m = sub_rating_re.match(line)
        if not m:
            continue
        lbl = m.group(1).strip().lower().replace(' ', '_')
        if lbl not in attrs:
            consumed.add(idx)
            attrs[lbl] = float(m.group(2))
        else:
            consumed.add(idx)

    # Pattern B
    for i in range(len(lines) - 1):
        if i in consumed or (i + 1) in consumed:
            continue
        
        raw_lbl = strip_ellipsis(lines[i].lower().strip())
        if raw_lbl not in label_map:
            continue
            
        val = strip_ellipsis(lines[i + 1].strip())
        if not val:
            continue
            
        if raw_lbl == 'service' and val.lower() not in dining_modes:
            continue
            
        key = label_map[raw_lbl]
        if key not in attrs:
            attrs[key] = val
            consumed.add(i)
            consumed.add(i + 1)
            
    return attrs

review1 = """Service
Dine in
Meal type
Lunch
Price per person
₹200–400
Food: 5
Service: 3
Atmosphere: 4
Wait time
No wait
Recommendation for vegetarians
Not sure
Vegetarian offerings
Vegetarian menu or section
Parking space
Not sure
Wheelchair accessibility
Yes
This is a good place, the food is great and the service is also good.
"""

review2 = """Service
Dine in
Meal type
Breakfast
Price per person
₹400–600
Food: 5
Service: 5
Atmosphere: 5
Group size
Suitable for all group sizes
This is a good place, the food is great and the service is also good.
"""

print("Review 1 Attributes:", simulate_extraction(review1))
print("Review 2 Attributes:", simulate_extraction(review2))
else:
    print("✅ All noise filtering tests passed!")
