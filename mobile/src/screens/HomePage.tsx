import { StyleSheet, Text, View } from "react-native";

import { AppButton } from "../components/ui/AppButton";
import { AppCard } from "../components/ui/AppCard";
import { AppScreen } from "../components/ui/AppScreen";
import { EmptyState } from "../components/ui/EmptyState";
import { SectionHeader } from "../components/ui/SectionHeader";

type HomePageProps = {
  activeProfileName: string;
  householdName: string;
  isSetupComplete: boolean;
  onGoToHousehold: () => void;
  onGoToMealPlan: () => void;
  profileCount: number;
};

const HIGHLIGHTS = [
  {
    meta: "Short video - 8 min",
    title: "Healthy meal prep basics",
  },
  {
    meta: "Article - Nutrition basics",
    title: "How to build a balanced plate",
  },
  {
    meta: "Video - Practical cooking",
    title: "Family-friendly dinner ideas",
  },
];

const KIDS_IDEAS = [
  "How to make vegetables more appealing for kids",
  "Simple colorful plate ideas",
  "Introducing new foods without pressure",
  "Easy snacks for busy family days",
];

const HABITS = [
  "Basic nutrition habits",
  "Simple grocery planning tips",
  "How to reduce food waste",
  "Planning meals when you are busy",
];

export function HomePage({
  activeProfileName,
  householdName,
  isSetupComplete,
  onGoToHousehold,
  onGoToMealPlan,
  profileCount,
}: HomePageProps) {
  if (!isSetupComplete) {
    return (
      <AppScreen>
        <EmptyState
          actionLabel="Go to Account Setup"
          onAction={onGoToHousehold}
          text="Create an account to start planning meals for your household."
          title="Set up your account first"
        />
      </AppScreen>
    );
  }

  return (
    <AppScreen>
      <AppCard>
        <View style={styles.headerCopy}>
          <Text style={styles.eyebrow}>Good evening, {activeProfileName}</Text>
          <Text style={styles.title}>{householdName}</Text>
          <Text style={styles.text}>
            {profileCount} profiles configured. Ready to plan balanced meals for your household.
          </Text>
        </View>
        <AppButton label="Go to Meal Plan" onPress={onGoToMealPlan} />
      </AppCard>

      <AppCard>
        <SectionHeader title="Daily Food Tip" />
        <Text style={styles.text}>
          In a rush? A simple snack like fruit, yogurt, or a small sandwich can help
          prevent impulsive food choices later.
        </Text>
      </AppCard>

      <View style={styles.section}>
        <SectionHeader title="Highlights of the Week" />
        <View style={styles.cardList}>
          {HIGHLIGHTS.map((item) => (
            <SmallInfoCard key={item.title} meta={item.meta} title={item.title} />
          ))}
        </View>
      </View>

      <View style={styles.section}>
        <SectionHeader title="Family & Kids Food Ideas" />
        <View style={styles.grid}>
          {KIDS_IDEAS.map((title) => (
            <SmallInfoCard key={title} title={title} />
          ))}
        </View>
      </View>

      <View style={styles.section}>
        <SectionHeader title="Learn More" meta="Healthy Habits" />
        <View style={styles.grid}>
          {HABITS.map((title) => (
            <SmallInfoCard key={title} title={title} />
          ))}
        </View>
      </View>
    </AppScreen>
  );
}

function SmallInfoCard({ meta, title }: { meta?: string; title: string }) {
  return (
    <AppCard>
      <Text style={styles.smallTitle}>{title}</Text>
      {meta ? <Text style={styles.meta}>{meta}</Text> : null}
    </AppCard>
  );
}

const styles = StyleSheet.create({
  cardList: {
    gap: 10,
  },
  eyebrow: {
    color: "#165D77",
    fontSize: 15,
    fontWeight: "800",
  },
  grid: {
    gap: 10,
  },
  headerCopy: {
    gap: 6,
  },
  meta: {
    color: "#6B7280",
    fontSize: 13,
    fontWeight: "700",
  },
  section: {
    gap: 10,
  },
  smallTitle: {
    color: "#111827",
    fontSize: 15,
    fontWeight: "800",
  },
  text: {
    color: "#4B5563",
    fontSize: 15,
    lineHeight: 21,
  },
  title: {
    color: "#111827",
    fontSize: 28,
    fontWeight: "900",
  },
});
