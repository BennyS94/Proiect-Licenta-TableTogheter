# Home assets

Home is a hardcoded discovery page. It should remain warm, clean, practical and household-oriented.

## Hero / welcome

Folder:
- `mobile/assets/home/welcome/`

Preferred:
- `cooking_lottie.json`

Fallback:
- `cooking_loop.webp`
- `cooking_loop.gif`

Guidelines:
- transparent background preferred
- should fit inside a 180 x 140 dp container
- animation should loop smoothly
- visual style should be warm, clean, cooking or family friendly
- avoid childish, medical or fitness-heavy visuals

Current behavior:
- Home uses `mobile/assets/home/welcome/cooking_lottie.json` through `lottie-react-native`.
- Missing optional non-hero custom assets must not crash the page.
- If replacing the hero animation later, keep the file small and preserve the same container sizing.

## Daily Food Tips

Folder:
- `mobile/assets/home/tips/`

Current active files:
- `tip_fruit_choice.png`
- `tip_arranged_plate.png`
- `tip_backup_meal.png`
- `tip_kids_tastes.png`

Recommended source size:
- 512 x 512 PNG/WebP
- transparent background preferred
- displayed around 96 x 96 dp

## Highlights

Folder:
- `mobile/assets/home/highlights/`

Expected later:
- `highlight_meal_prep.png`
- `highlight_balanced_plate.png`
- `highlight_family_dinner.png`

Recommended source size:
- 640 x 360 or 800 x 450
- displayed cropped around 124 x 94 dp

## Family and kids

Folder:
- `mobile/assets/home/family_kids/`

Expected later:
- `kids_vegetables.png`
- `kids_colorful_plate.png`
- `kids_new_foods.png`
- `kids_snacks.png`

## Healthy habits

Folder:
- `mobile/assets/home/healthy_habits/`

Expected later:
- `habit_basic_nutrition.png`
- `habit_grocery_planning.png`
- `habit_reduce_waste.png`
- `habit_busy_day.png`
