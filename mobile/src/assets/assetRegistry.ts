export const expectedHomeAssets = {
  welcome: {
    lottie: "mobile/assets/home/welcome/cooking_lottie.json",
    fallbackGif: "mobile/assets/home/welcome/cooking_loop.gif",
    fallbackWebp: "mobile/assets/home/welcome/cooking_loop.webp",
  },
  tips: {
    arrangedPlate: "mobile/assets/home/tips/tip_arranged_plate.png",
    backupMeal: "mobile/assets/home/tips/tip_backup_meal.png",
    fruitChoice: "mobile/assets/home/tips/tip_fruit_choice.png",
    kidsTastes: "mobile/assets/home/tips/tip_kids_tastes.png",
  },
  highlights: {
    balancedPlate: "mobile/assets/home/highlights/highlight_balanced_plate.png",
    familyDinner: "mobile/assets/home/highlights/highlight_family_dinner.png",
    mealPrep: "mobile/assets/home/highlights/highlight_meal_prep.png",
  },
  familyKids: {
    colorfulPlate: "mobile/assets/home/family_kids/kids_colorful_plate.png",
    newFoods: "mobile/assets/home/family_kids/kids_new_foods.png",
    snacks: "mobile/assets/home/family_kids/kids_snacks.png",
    vegetables: "mobile/assets/home/family_kids/kids_vegetables.png",
  },
  healthyHabits: {
    basicNutrition: "mobile/assets/home/healthy_habits/habit_basic_nutrition.png",
    busyDay: "mobile/assets/home/healthy_habits/habit_busy_day.png",
    groceryPlanning: "mobile/assets/home/healthy_habits/habit_grocery_planning.png",
    reduceWaste: "mobile/assets/home/healthy_habits/habit_reduce_waste.png",
  },
} as const;

export const expectedNavigationAssets = {
  home: "mobile/assets/navigation/nav_home.png",
  household: "mobile/assets/navigation/nav_household.png",
  insights: "mobile/assets/navigation/nav_insights.png",
  mealPlan: "mobile/assets/navigation/nav_meal_plan.png",
} as const;

export const expectedGroceryPackageAssets = {
  bag: "mobile/assets/grocery/package_icons/package_bag.png",
  bottle: "mobile/assets/grocery/package_icons/package_bottle.png",
  carton: "mobile/assets/grocery/package_icons/package_carton.png",
  pack: "mobile/assets/grocery/package_icons/package_pack.png",
  pantry: "mobile/assets/grocery/package_icons/package_pantry.png",
  piece: "mobile/assets/grocery/package_icons/package_piece.png",
  scale: "mobile/assets/grocery/package_icons/package_scale.png",
  tub: "mobile/assets/grocery/package_icons/package_tub.png",
  warning: "mobile/assets/grocery/package_icons/package_warning.png",
} as const;

export const expectedInsightsAssets = {
  macro: {
    carbsWheat: "mobile/assets/insights/macro/carbs_wheat.png",
    fatsAvocado: "mobile/assets/insights/macro/fats_avocado.png",
    proteinDrumstick: "mobile/assets/insights/macro/protein_drumstick.png",
  },
  meals: {
    breakfastCoffee: "mobile/assets/insights/meals/breakfast_coffee.svg",
    dinnerPlateCutlery: "mobile/assets/insights/meals/dinner_plate_cutlery.svg",
    lunchServingDome: "mobile/assets/insights/meals/lunch_serving_dome.svg",
    snackApple: "mobile/assets/insights/meals/snack_apple.svg",
  },
} as const;
