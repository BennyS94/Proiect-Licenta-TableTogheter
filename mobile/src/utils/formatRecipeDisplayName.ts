export function formatRecipeDisplayName(value: string): string {
  return value.replace(/\band\b/gi, "&");
}
