export type HomeResourceKind = "article" | "video" | "tip";

export type HomeResourceItem = {
  description: string;
  id: string;
  image: string;
  imageTone: string;
  kind: HomeResourceKind;
  title: string;
  typeLabel: string;
  url?: string;
};

export type DailyFoodTip = {
  body: string;
  id: string;
  image: string;
  imageTone: string;
  title: string;
};

export const dailyFoodTips: DailyFoodTip[] = [
  {
    body: "Pair fruit with yogurt, nuts, or cheese for a snack that feels more filling.",
    id: "balanced-snack",
    image: "mobile/assets/home/tips/tip_snack.png",
    imageTone: "#F7C948",
    title: "Balanced snacks",
  },
  {
    body: "A planned snack can help you avoid rushed food choices later.",
    id: "family-compromise",
    image: "mobile/assets/home/tips/tip_compromise.png",
    imageTone: "#74B72E",
    title: "Shared meals",
  },
  {
    body: "Add one colorful food to the plate. Small changes make meals easier.",
    id: "colorful-plate",
    image: "mobile/assets/home/tips/tip_colorful_plate.png",
    imageTone: "#F97316",
    title: "Colorful plates",
  },
  {
    body: "Plan one simple meal ahead to make busy days less stressful.",
    id: "planning",
    image: "mobile/assets/home/tips/tip_planning.png",
    imageTone: "#38BDF8",
    title: "Simple planning",
  },
  {
    body: "Keep one easy backup meal at home for days when cooking feels too much.",
    id: "backup-meal",
    image: "mobile/assets/home/tips/tip_backup_meal.png",
    imageTone: "#F7C948",
    title: "Backup meal",
  },
  {
    body: "For kids, small low-pressure tastes still count as progress.",
    id: "kids-progress",
    image: "mobile/assets/home/tips/tip_kids_progress.png",
    imageTone: "#74B72E",
    title: "Kids progress",
  },
];

export const weeklyHighlights: HomeResourceItem[] = [
  {
    description: "Simple prep blocks that make weeknight plates faster for the whole household.",
    id: "meal-prep-basics",
    image: "mobile/assets/home/highlights/highlight_meal_prep.png",
    imageTone: "#F7C948",
    kind: "video",
    title: "Healthy meal prep basics",
    typeLabel: "Short video · 8 min",
    url: "https://www.youtube.com/results?search_query=healthy+meal+prep+basics",
  },
  {
    description: "A quick guide to combining protein, grains, vegetables, and healthy fats.",
    id: "balanced-plate",
    image: "mobile/assets/home/highlights/highlight_balanced_plate.png",
    imageTone: "#74B72E",
    kind: "article",
    title: "How to build a balanced plate",
    typeLabel: "Article · Nutrition basics",
    url: "https://www.myplate.gov/eat-healthy/what-is-myplate",
  },
  {
    description: "Practical dinner ideas that can work for adults, kids, and different goals.",
    id: "family-dinner",
    image: "mobile/assets/home/highlights/highlight_family_dinner.png",
    imageTone: "#F97316",
    kind: "video",
    title: "Family-friendly dinner ideas",
    typeLabel: "Video · Practical cooking",
    url: "https://www.youtube.com/results?search_query=family+friendly+dinner+ideas",
  },
];

export const familyKidsIdeas: HomeResourceItem[] = [
  {
    description: "Small presentation changes that can make vegetables feel more approachable.",
    id: "kids-vegetables",
    image: "mobile/assets/home/family_kids/kids_vegetables.png",
    imageTone: "#74B72E",
    kind: "article",
    title: "How to make vegetables more appealing for kids",
    typeLabel: "Article · Family meals",
    url: "https://www.myplate.gov/tip-sheet/kid-friendly-veggies-and-fruits",
  },
  {
    description: "Use color and simple shapes to make everyday plates easier to enjoy.",
    id: "kids-colorful-plate",
    image: "mobile/assets/home/family_kids/kids_colorful_plate.png",
    imageTone: "#38BDF8",
    kind: "tip",
    title: "Simple colorful plate ideas",
    typeLabel: "Tip · Plate ideas",
    url: "https://www.myplate.gov/eat-healthy",
  },
  {
    description: "Gentle ways to offer unfamiliar foods without turning dinner into a battle.",
    id: "kids-new-foods",
    image: "mobile/assets/home/family_kids/kids_new_foods.png",
    imageTone: "#F97316",
    kind: "article",
    title: "Introducing new foods without pressure",
    typeLabel: "Article · Kids",
    url: "https://www.youtube.com/results?search_query=introducing+new+foods+to+kids",
  },
  {
    description: "Fast snack ideas that are easy to adapt for different ages and appetites.",
    id: "kids-snacks",
    image: "mobile/assets/home/family_kids/kids_snacks.png",
    imageTone: "#F7C948",
    kind: "tip",
    title: "Easy snacks for busy family days",
    typeLabel: "Tip · Snacks",
    url: "https://www.myplate.gov/eat-healthy/healthy-eating-budget",
  },
];

export const healthyHabits: HomeResourceItem[] = [
  {
    description: "Start with repeatable habits: regular meals, protein, plants, and hydration.",
    id: "basic-nutrition",
    image: "mobile/assets/home/healthy_habits/habit_basic_nutrition.png",
    imageTone: "#74B72E",
    kind: "article",
    title: "Basic nutrition habits",
    typeLabel: "Article · Everyday basics",
    url: "https://www.myplate.gov/eat-healthy",
  },
  {
    description: "Build a short shopping list from meals you already know your household likes.",
    id: "grocery-planning",
    image: "mobile/assets/home/healthy_habits/habit_grocery_planning.png",
    imageTone: "#38BDF8",
    kind: "tip",
    title: "Simple grocery planning tips",
    typeLabel: "Tip · Shopping",
    url: "https://www.myplate.gov/eat-healthy/healthy-eating-budget",
  },
  {
    description: "Use leftovers, flexible sides, and freezer staples to waste less food.",
    id: "reduce-waste",
    image: "mobile/assets/home/healthy_habits/habit_reduce_waste.png",
    imageTone: "#F97316",
    kind: "article",
    title: "How to reduce food waste",
    typeLabel: "Article · Kitchen habits",
    url: "https://www.usda.gov/foodlossandwaste",
  },
  {
    description: "Keep a few quick meal templates ready for weeks when time is tight.",
    id: "busy-day",
    image: "mobile/assets/home/healthy_habits/habit_busy_day.png",
    imageTone: "#F7C948",
    kind: "tip",
    title: "Planning meals when you are busy",
    typeLabel: "Tip · Busy days",
    url: "https://www.youtube.com/results?search_query=quick+healthy+family+meal+planning",
  },
];
