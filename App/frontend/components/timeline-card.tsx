"use client";

import { useState } from "react";
import { Card, CardContent, CardHeader } from "@/components/ui/card";
import {
    Collapsible,
    CollapsibleContent,
    CollapsibleTrigger,
} from "@/components/ui/collapsible";
import { ChevronDown } from "lucide-react";
import { TimelineCluster } from "@/lib/services/timeline";

interface TimelineCardProps {
    cluster: TimelineCluster;
}

// Political leaning colors
const LEANING_COLORS: Record<string, string> = {
    left: "#3B82F6", // blue
    centre_left: "#60A5FA", // light blue
    centre: "#9CA3AF", // gray
    centre_right: "#F97316", // orange
    right: "#EF4444", // red
};

const LEANING_LABELS: Record<string, string> = {
    left: "Links",
    centre_left: "C-Links",
    centre: "Centrum",
    centre_right: "C-Rechts",
    right: "Rechts",
};

// Parse owner_reach JSON string and display as stacked bar
interface OwnerReach {
    name: string;
    percentage: number;
}

function parseOwnerReach(ownerReachStr: string | null): OwnerReach[] {
    if (!ownerReachStr) return [];
    try {
        const parsed = JSON.parse(ownerReachStr);
        if (typeof parsed === "object" && parsed !== null) {
            return Object.entries(parsed)
                .map(([name, value]) => ({
                    name,
                    percentage: Number(value) * 100,
                }))
                .filter((item) => item.percentage > 0)
                .sort((a, b) => b.percentage - a.percentage);
        }
    } catch {
        return [];
    }
    return [];
}

// Parse articles JSON string
interface Article {
    title: string;
    feed: string;
    url?: string;
}

function parseArticles(articlesStr: string | null): Article[] {
    if (!articlesStr) return [];
    try {
        return JSON.parse(articlesStr);
    } catch {
        return [];
    }
}

// Owner colors
const OWNER_COLORS: Record<string, string> = {
    "DPG Media": "#3B82F6",
    "Mediahuis NL": "#F97316",
    NPO: "#60A5FA",
    Talpa: "#10B981",
    default: "#6B7280",
};

function getOwnerColor(name: string): string {
    return OWNER_COLORS[name] || OWNER_COLORS.default;
}

export function TimelineCard({ cluster }: TimelineCardProps) {
    const [isOpen, setIsOpen] = useState(false);

    // Build political leaning data
    const leanings = [
        { key: "left", value: cluster.left },
        { key: "centre_left", value: cluster.centre_left },
        { key: "centre", value: cluster.centre },
        { key: "centre_right", value: cluster.centre_right },
        { key: "right", value: cluster.right },
    ].filter((l) => l.value && l.value > 0);

    const totalLeaning = leanings.reduce((sum, l) => sum + (l.value || 0), 0);

    const ownerReach = parseOwnerReach(cluster.owner_reach);
    const articles = parseArticles(cluster.articles);

    const formatDate = (date: Date | null) => {
        if (!date) return "";
        return new Date(date).toLocaleDateString("nl-NL", {
            day: "2-digit",
            month: "2-digit",
            year: "numeric",
            hour: "2-digit",
            minute: "2-digit",
        });
    };

    return (
        <Card className="mb-4 overflow-hidden">
            <CardHeader className="pb-3">
                <h3 className="text-lg font-semibold leading-tight text-gray-900">
                    {cluster.title || "Onbekend onderwerp"}
                </h3>
                <p className="text-sm text-gray-500">
                    {cluster.num_articles} nieuwsberichten &middot; {cluster.num_feeds}{" "}
                    bronnen &middot; {formatDate(cluster.max_published_date)}
                </p>
            </CardHeader>

            <CardContent className="pt-0">
                {/* Political Leaning Bar */}
                {totalLeaning > 0 && (
                    <div className="mb-3">
                        <div className="flex h-7 w-full overflow-hidden rounded-md">
                            {leanings.map((l) => {
                                const percentage = ((l.value || 0) / totalLeaning) * 100;
                                if (percentage < 5) return null;
                                return (
                                    <div
                                        key={l.key}
                                        className="flex items-center justify-center text-xs font-medium text-white"
                                        style={{
                                            width: `${percentage}%`,
                                            backgroundColor: LEANING_COLORS[l.key],
                                        }}
                                    >
                                        {percentage >= 10 &&
                                            `${LEANING_LABELS[l.key]} ${Math.round(percentage)}%`}
                                    </div>
                                );
                            })}
                        </div>
                    </div>
                )}

                {/* Owner Reach Bar */}
                {ownerReach.length > 0 && (
                    <div className="mb-3">
                        <div className="flex h-7 w-full overflow-hidden rounded-md">
                            {ownerReach.map((owner) => {
                                if (owner.percentage < 5) return null;
                                return (
                                    <div
                                        key={owner.name}
                                        className="flex items-center justify-center text-xs font-medium text-white"
                                        style={{
                                            width: `${owner.percentage}%`,
                                            backgroundColor: getOwnerColor(owner.name),
                                        }}
                                    >
                                        {owner.percentage >= 10 &&
                                            `${owner.name} ${Math.round(owner.percentage)}%`}
                                    </div>
                                );
                            })}
                        </div>
                    </div>
                )}

                {/* Expandable Articles */}
                {articles.length > 0 && (
                    <Collapsible open={isOpen} onOpenChange={setIsOpen}>
                        <CollapsibleTrigger className="flex w-full items-center justify-center gap-1 py-2 text-sm font-medium text-blue-600 hover:text-blue-800">
                            Bekijk nieuwsberichten
                            <ChevronDown
                                className={`h-4 w-4 transition-transform ${isOpen ? "rotate-180" : ""}`}
                            />
                        </CollapsibleTrigger>
                        <CollapsibleContent>
                            <div className="mt-2 space-y-2 border-t pt-3">
                                {articles.map((article, idx) => (
                                    <div key={idx} className="text-sm">
                                        <span className="font-medium text-gray-700">
                                            [{article.feed}]
                                        </span>{" "}
                                        {article.url ? (
                                            <a
                                                href={article.url}
                                                target="_blank"
                                                rel="noopener noreferrer"
                                                className="text-blue-600 hover:underline"
                                            >
                                                {article.title}
                                            </a>
                                        ) : (
                                            <span className="text-gray-900">{article.title}</span>
                                        )}
                                    </div>
                                ))}
                            </div>
                        </CollapsibleContent>
                    </Collapsible>
                )}
            </CardContent>
        </Card>
    );
}
