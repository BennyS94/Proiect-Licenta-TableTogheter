import { useState } from "react";
import type { NativeScrollEvent, NativeSyntheticEvent } from "react-native";
import { Image, Linking, Pressable, ScrollView, StyleSheet, Text, View } from "react-native";

import type { HomeResourceItem } from "../../data/homeContent";
import { ChevronDownIcon } from "../icons/ChevronDownIcon";
import { VideoPlayBadge } from "./VideoPlayBadge";

type ResourceCarouselSectionProps = {
  contentWidth: number;
  items: HomeResourceItem[];
  onSeeAll: () => void;
  title: string;
};

export function ResourceCarouselSection({
  contentWidth,
  items,
  onSeeAll,
  title,
}: ResourceCarouselSectionProps) {
  const [activeIndex, setActiveIndex] = useState(0);
  const thumbnailWidth = contentWidth >= 340 ? 124 : 112;

  function handleScroll(event: NativeSyntheticEvent<NativeScrollEvent>) {
    if (!contentWidth) {
      return;
    }
    const nextIndex = Math.round(event.nativeEvent.contentOffset.x / contentWidth);
    setActiveIndex(Math.max(0, Math.min(items.length - 1, nextIndex)));
  }

  return (
    <View style={[styles.section, { width: contentWidth }]}>
      <View style={styles.headerRow}>
        <Text style={styles.sectionTitle}>{title}</Text>
        <Pressable
          accessibilityRole="button"
          onPress={onSeeAll}
          style={({ pressed }) => [styles.seeAllButton, pressed ? styles.pressed : null]}
        >
          <Text style={styles.seeAllText}>See all</Text>
          <View style={styles.chevronRight}>
            <ChevronDownIcon color="#74B72E" size={15} />
          </View>
        </Pressable>
      </View>

      <ScrollView
        decelerationRate="fast"
        horizontal
        onScroll={handleScroll}
        pagingEnabled
        scrollEventThrottle={16}
        showsHorizontalScrollIndicator={false}
        snapToInterval={contentWidth}
      >
        {items.map((item) => (
          <ResourceCard
            contentWidth={contentWidth}
            item={item}
            key={item.id}
            thumbnailWidth={thumbnailWidth}
          />
        ))}
      </ScrollView>

      <View style={styles.dotsRow}>
        {items.map((item, index) => (
          <View
            key={item.id}
            style={[styles.dot, index === activeIndex ? styles.dotActive : null]}
          />
        ))}
      </View>
    </View>
  );
}

function ResourceCard({
  contentWidth,
  item,
  thumbnailWidth,
}: {
  contentWidth: number;
  item: HomeResourceItem;
  thumbnailWidth: number;
}) {
  const canOpen = Boolean(item.url);

  return (
    <Pressable
      accessibilityRole="button"
      disabled={!canOpen}
      onPress={() => openExternalUrl(item.url)}
      style={({ pressed }) => [
        styles.card,
        { width: contentWidth },
        pressed && canOpen ? styles.pressed : null,
      ]}
    >
      <View
        style={[
          styles.thumbnail,
          {
            backgroundColor: item.imageTone,
            width: thumbnailWidth,
          },
        ]}
      >
        {item.imageAsset ? (
          <Image resizeMode="cover" source={item.imageAsset} style={styles.thumbnailImage} />
        ) : (
          <>
            <View style={styles.thumbnailPlate} />
            <View style={styles.thumbnailLeaf} />
            <View style={styles.thumbnailFork} />
          </>
        )}
        {item.kind === "video" ? <VideoPlayBadge /> : null}
      </View>

      <View style={styles.cardText}>
        <Text numberOfLines={2} style={styles.cardTitle}>
          {item.title}
        </Text>
        <Text numberOfLines={2} style={styles.description}>
          {item.description}
        </Text>
        <Text numberOfLines={1} style={styles.typeLabel}>
          {item.typeLabel}
        </Text>
      </View>
    </Pressable>
  );
}

function openExternalUrl(url?: string) {
  if (!url) {
    return;
  }
  void Linking.openURL(url).catch(() => undefined);
}

const styles = StyleSheet.create({
  card: {
    backgroundColor: "#FFFFFF",
    borderColor: "#E5E7EB",
    borderRadius: 22,
    borderWidth: 1,
    elevation: 2,
    flexDirection: "row",
    height: 130,
    padding: 12,
    shadowColor: "#1F2933",
    shadowOffset: { width: 0, height: 7 },
    shadowOpacity: 0.08,
    shadowRadius: 14,
  },
  cardText: {
    flex: 1,
    justifyContent: "center",
    marginLeft: 14,
  },
  cardTitle: {
    color: "#1F2933",
    fontSize: 16,
    fontWeight: "700",
    lineHeight: 20,
  },
  chevronRight: {
    transform: [{ rotate: "-90deg" }],
  },
  description: {
    color: "#6B7280",
    fontSize: 13.5,
    lineHeight: 18,
    marginTop: 5,
  },
  dot: {
    backgroundColor: "#D6D8D2",
    borderRadius: 4,
    height: 7,
    width: 7,
  },
  dotActive: {
    backgroundColor: "#74B72E",
    height: 8,
    width: 8,
  },
  dotsRow: {
    alignItems: "center",
    flexDirection: "row",
    gap: 7,
    height: 12,
    justifyContent: "center",
    marginTop: 6,
  },
  headerRow: {
    alignItems: "center",
    flexDirection: "row",
    height: 30,
    justifyContent: "space-between",
    marginBottom: 8,
  },
  pressed: {
    opacity: 0.82,
  },
  section: {
    marginTop: 14,
  },
  sectionTitle: {
    color: "#1F2933",
    flex: 1,
    fontSize: 21,
    fontWeight: "800",
    lineHeight: 26,
  },
  seeAllButton: {
    alignItems: "center",
    flexDirection: "row",
    gap: 2,
    paddingLeft: 10,
  },
  seeAllText: {
    color: "#74B72E",
    fontSize: 16,
    fontWeight: "700",
  },
  thumbnail: {
    alignItems: "center",
    borderRadius: 16,
    height: 100,
    justifyContent: "center",
    overflow: "hidden",
  },
  thumbnailImage: {
    height: "100%",
    width: "100%",
  },
  thumbnailFork: {
    backgroundColor: "rgba(255,255,255,0.75)",
    borderRadius: 2,
    height: 50,
    position: "absolute",
    right: 22,
    top: 24,
    width: 5,
  },
  thumbnailLeaf: {
    backgroundColor: "rgba(255,255,255,0.7)",
    borderBottomLeftRadius: 18,
    borderTopRightRadius: 18,
    height: 34,
    position: "absolute",
    right: 34,
    top: 18,
    transform: [{ rotate: "-18deg" }],
    width: 48,
  },
  thumbnailPlate: {
    backgroundColor: "rgba(255,255,255,0.82)",
    borderRadius: 32,
    height: 64,
    width: 64,
  },
  typeLabel: {
    color: "#6B7280",
    fontSize: 13,
    marginTop: 6,
  },
});
