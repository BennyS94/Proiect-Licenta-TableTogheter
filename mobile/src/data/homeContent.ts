import type { ImageSourcePropType } from "react-native";

export type HomeResourceKind = "article" | "video" | "tip";

export type HomeResourceItem = {
  description: string;
  id: string;
  image: string;
  imageAsset?: ImageSourcePropType;
  imageTone: string;
  kind: HomeResourceKind;
  title: string;
  typeLabel: string;
  url?: string;
};

export type DailyFoodTip = {
  body: string;
  id: string;
  image: ImageSourcePropType;
  imageTone: string;
  title: string;
};

const tipFruitChoice = require("../../assets/home/tips/tip_fruit_choice.png") as ImageSourcePropType;
const tipArrangedPlate = require("../../assets/home/tips/tip_arranged_plate.png") as ImageSourcePropType;
const tipBackupMeal = require("../../assets/home/tips/tip_backup_meal.png") as ImageSourcePropType;
const tipKidsTastes = require("../../assets/home/tips/tip_kids_tastes.png") as ImageSourcePropType;
const highlightWeeklyMealPrep = require("../../assets/home/highlights/highlight_weekly_meal_prep.png") as ImageSourcePropType;
const highlightBalancedPlateBasics = require("../../assets/home/highlights/highlight_balanced_plate_basics.png") as ImageSourcePropType;
const highlightPracticalCookingTips = require("../../assets/home/highlights/highlight_practical_cooking_tips.png") as ImageSourcePropType;
const highlightEasyDinnerIdeas = require("../../assets/home/highlights/highlight_easy_dinner_ideas.png") as ImageSourcePropType;
const familyMakeVegetablesAppealing = require("../../assets/home/family_kids/family_make_vegetables_appealing.png") as ImageSourcePropType;
const familyHelpKidsEnjoyVegetables = require("../../assets/home/family_kids/family_help_kids_enjoy_vegetables.png") as ImageSourcePropType;
const familyFunFruitVeggieShapes = require("../../assets/home/family_kids/family_fun_fruit_veggie_shapes.png") as ImageSourcePropType;
const familyPickyEatersNewFoods = require("../../assets/home/family_kids/family_picky_eaters_new_foods.png") as ImageSourcePropType;
const habitBasicNutrition = require("../../assets/home/healthy_habits/habit_basic_nutrition.png") as ImageSourcePropType;
const habitSimpleMealPlanning = require("../../assets/home/healthy_habits/habit_simple_meal_planning.png") as ImageSourcePropType;
const habitReduceFoodWaste = require("../../assets/home/healthy_habits/habit_reduce_food_waste.png") as ImageSourcePropType;
const habitBalancedEatingSimple = require("../../assets/home/healthy_habits/habit_balanced_eating_simple.png") as ImageSourcePropType;

export const dailyFoodTips: DailyFoodTip[] = [
  {
    body: "Choosing a piece of fruit is better than staying hungry while dieting.",
    id: "fruit-while-dieting",
    image: tipFruitChoice,
    imageTone: "#F7C948",
    title: "Fruit while dieting",
  },
  {
    body: "A well-arranged plate can make eating feel easier and more enjoyable.",
    id: "well-arranged-plate",
    image: tipArrangedPlate,
    imageTone: "#74B72E",
    title: "Plate arrangement",
  },
  {
    body: "Keeping a backup meal in the fridge is helpful for days when cooking feels like too much.",
    id: "fridge-backup-meal",
    image: tipBackupMeal,
    imageTone: "#F97316",
    title: "Backup meal",
  },
  {
    body: "For kids, even small low-pressure tastes count as progress.",
    id: "kids-low-pressure-tastes",
    image: tipKidsTastes,
    imageTone: "#38BDF8",
    title: "Kids progress",
  },
];

export const weeklyHighlights: HomeResourceItem[] = [
  {
    description: "Useful healthy cooking ideas you can apply in everyday meals.",
    id: "practical-cooking-tips",
    image: "mobile/assets/home/highlights/highlight_practical_cooking_tips.png",
    imageAsset: highlightPracticalCookingTips,
    imageTone: "#F97316",
    kind: "video",
    title: "Practical cooking tips",
    typeLabel: "Video \u00B7 Healthy cooking",
    url: "https://www.youtube.com/watch?v=agSdpgxgG54&t=60s",
  },
  {
    description: "Simple prep ideas for practical healthy meals during the week.",
    id: "weekly-healthy-meal-prep",
    image: "mobile/assets/home/highlights/highlight_weekly_meal_prep.png",
    imageAsset: highlightWeeklyMealPrep,
    imageTone: "#F7C948",
    kind: "video",
    title: "Weekly healthy meal prep",
    typeLabel: "Video \u00B7 Meal prep",
    url: "https://www.youtube.com/watch?v=AYXfaVD5o40",
  },
  {
    description: "Easy balanced meal ideas built around simple food groups.",
    id: "balanced-plate-basics",
    image: "mobile/assets/home/highlights/highlight_balanced_plate_basics.png",
    imageAsset: highlightBalancedPlateBasics,
    imageTone: "#74B72E",
    kind: "video",
    title: "Balanced plate basics",
    typeLabel: "Video \u00B7 Nutrition basics",
    url: "https://www.youtube.com/watch?v=R66PnZoAjQg",
  },
  {
    description: "Practical dinner inspiration for simple family-friendly meals.",
    id: "easy-dinner-ideas",
    image: "mobile/assets/home/highlights/highlight_easy_dinner_ideas.png",
    imageAsset: highlightEasyDinnerIdeas,
    imageTone: "#38BDF8",
    kind: "video",
    title: "Easy dinner ideas",
    typeLabel: "Video \u00B7 Dinner ideas",
    url: "https://www.youtube.com/watch?v=FOvHpMkC_XI",
  },
];

export const familyKidsIdeas: HomeResourceItem[] = [
  {
    description: "Gentle ways to introduce healthy foods without pressure.",
    id: "help-picky-eaters-try-new-foods",
    image: "mobile/assets/home/family_kids/family_picky_eaters_new_foods.png",
    imageAsset: familyPickyEatersNewFoods,
    imageTone: "#F7C948",
    kind: "article",
    title: "Help picky eaters try new foods",
    typeLabel: "Article \u00B7 Picky eating",
    url: "https://www.healthychildren.org/English/tips-tools/ask-the-pediatrician/Pages/How-Do-I-Help-My-Picky-Eater-Try-More-Foods.aspx",
  },
  {
    description: "Small presentation changes that can help vegetables feel more inviting for kids.",
    id: "make-vegetables-more-appealing",
    image: "mobile/assets/home/family_kids/family_make_vegetables_appealing.png",
    imageAsset: familyMakeVegetablesAppealing,
    imageTone: "#74B72E",
    kind: "video",
    title: "Make vegetables more appealing",
    typeLabel: "Video \u00B7 Family meals",
    url: "https://www.youtube.com/watch?v=MZDA2pdTnqE",
  },
  {
    description: "Practical ideas for helping children accept and enjoy more vegetables.",
    id: "help-kids-enjoy-vegetables",
    image: "mobile/assets/home/family_kids/family_help_kids_enjoy_vegetables.png",
    imageAsset: familyHelpKidsEnjoyVegetables,
    imageTone: "#38BDF8",
    kind: "article",
    title: "Help kids enjoy vegetables",
    typeLabel: "Article \u00B7 Family meals",
    url: "https://www.lizshealthytable.com/2018/04/02/19-ways-get-kids-eat-love-vegetables/",
  },
  {
    description: "Playful lunchbox ideas using fruit and vegetable shapes kids may enjoy.",
    id: "fun-fruit-veggie-shapes",
    image: "mobile/assets/home/family_kids/family_fun_fruit_veggie_shapes.png",
    imageAsset: familyFunFruitVeggieShapes,
    imageTone: "#F97316",
    kind: "article",
    title: "Fun fruit & veggie shapes",
    typeLabel: "Article \u00B7 Kids lunch ideas",
    url: "https://healthyfamilyproject.com/fun-fruit-veggie-shapes-lunchbox/",
  },
];

export const healthyHabits: HomeResourceItem[] = [
  {
    description: "Plan meals ahead with simple food group variety.",
    id: "simple-meal-planning-tips",
    image: "mobile/assets/home/healthy_habits/habit_simple_meal_planning.png",
    imageAsset: habitSimpleMealPlanning,
    imageTone: "#38BDF8",
    kind: "article",
    title: "Simple meal planning tips",
    typeLabel: "Article \u00B7 Meal planning",
    url: "https://healthyfamilyproject.com/10-beginners-tips-meal-planning-like-pro/",
  },
  {
    description: "Simple ideas for building balanced everyday meals.",
    id: "basic-nutrition",
    image: "mobile/assets/home/healthy_habits/habit_basic_nutrition.png",
    imageAsset: habitBasicNutrition,
    imageTone: "#74B72E",
    kind: "article",
    title: "Basic nutrition habits",
    typeLabel: "Article \u00B7 Everyday basics",
    url: "https://nutritionsource.hsph.harvard.edu/healthy-eating-plate/",
  },
  {
    description: "Small planning habits that help use food better.",
    id: "reduce-food-waste-at-home",
    image: "mobile/assets/home/healthy_habits/habit_reduce_food_waste.png",
    imageAsset: habitReduceFoodWaste,
    imageTone: "#F97316",
    kind: "article",
    title: "Reduce food waste at home",
    typeLabel: "Article \u00B7 Grocery habits",
    url: "https://www.fda.gov/food/consumers/tips-reduce-food-waste",
  },
  {
    description: "Simple principles for everyday balanced meals.",
    id: "balanced-eating-made-simple",
    image: "mobile/assets/home/healthy_habits/habit_balanced_eating_simple.png",
    imageAsset: habitBalancedEatingSimple,
    imageTone: "#F7C948",
    kind: "article",
    title: "Balanced eating made simple",
    typeLabel: "Article \u00B7 Healthy eating",
    url: "https://www.nhs.uk/live-well/eat-well/how-to-eat-a-balanced-diet/eating-a-balanced-diet/",
  },
];
