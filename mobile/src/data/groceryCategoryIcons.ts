import type { ImageSourcePropType } from "react-native";

export const groceryCategoryIcons = {
  carbs_grains: require("../../assets/grocery/categories/carbs_grains.png") as ImageSourcePropType,
  dairy_eggs: require("../../assets/grocery/categories/dairy_eggs.png") as ImageSourcePropType,
  fruits: require("../../assets/grocery/categories/fruits.png") as ImageSourcePropType,
  legumes_beans: require("../../assets/grocery/categories/legumes_beans.png") as ImageSourcePropType,
  meat_fish: require("../../assets/grocery/categories/meat_fish.png") as ImageSourcePropType,
  oils_fats: require("../../assets/grocery/categories/oils_fats.png") as ImageSourcePropType,
  other_review: require("../../assets/grocery/categories/other_review.png") as ImageSourcePropType,
  pantry_basics: require("../../assets/grocery/categories/pantry_basics.png") as ImageSourcePropType,
  sauces_canned: require("../../assets/grocery/categories/sauces_canned.png") as ImageSourcePropType,
  seasonings_spices: require("../../assets/grocery/categories/seasonings_spices.png") as ImageSourcePropType,
  sweeteners: require("../../assets/grocery/categories/sweeteners.png") as ImageSourcePropType,
  vegetables: require("../../assets/grocery/categories/vegetables.png") as ImageSourcePropType,
} satisfies Record<string, ImageSourcePropType>;

export function groceryCategoryIconForKey(categoryKey: string): ImageSourcePropType {
  return groceryCategoryIcons[categoryKey as keyof typeof groceryCategoryIcons] ?? groceryCategoryIcons.other_review;
}
