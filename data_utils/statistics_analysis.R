gen_bins <- function(data, buckets) {
  max <- round(max(data) + 50, -2)
  if (max > max(buckets))
    buckets <-  c(buckets, max)
  labels <- rep(0, length(buckets) - 1)
  for (i in 1:length(buckets) - 1) {
    if (i >= length(buckets)-1)
      next_ <- "more"
    else 
      next_ <- as.character(buckets[i+1])
    label <- paste("[",
                   as.character(buckets[i]),
                   "-",
                   next_,
                   ")", sep = "")
    labels[i] <- label
  }
  bins <- cut(data, buckets, labels = labels)
  return(bins)
}

draw_barplot <- function(data, bin_intervals, xlab, ylab, axis_interleaving) {
  bins <- gen_bins(data, bin_intervals)
  tab <- table(bins)
  barplot(tab,
          xlab = xlab,
          ylab = ylab,
          axes = F,
          cex.names = 1.3,
          cex.lab = 1.5,
          cex.axis = 1.3)
  axis(2, at= seq(0, max(tab), by = axis_interleaving))
}

data <- read.csv("statistics.csv", sep = ",", header = T,
                stringsAsFactors = F)

links_intervals <- c(0, 100, 250, 500, 750, 1000)
links <- data$links
revs_intervals <- c(0, 100, 250, 500, 750, 1000,
                    1500, 2000, 2500)
revs <- data$revs

avgtok_intervals <- c(0, 1000, 1500, 2000, 3000, 4000,
                5000)
avg_tokens <- data$avg_tokens

pdf("statistics1.pdf",
    width=5, height=6)
par(mfrow=c(3,1))

draw_barplot(revs/100, revs_intervals/100,
             "revisions per page (times 100)", "number of pages",
             5)
draw_barplot(links/100, links_intervals/100,
            "links per page (times 100)", "number of pages",
            10)
draw_barplot(avg_tokens/1000, avgtok_intervals/1000,
             "avg. tokens per page (times 1000)",
             "number of pages",
             10)
dev.off()

cat("File statistics1.pdf printed!\n")
# statistics2
revs_ins <- data$revs[data$inss != 0]
revs_del <- data$revs[data$dels != 0]
inss <- data$inss[data$inss != 0]
dels <- data$dels[data$dels != 0]
ins_ratio <- revs_ins / (revs_ins + inss)
del_ratio <- revs_del / (revs_del + dels)

del_ins_ratio <- data$removed_tokens / data$added_tokens

pdf("statistics2a.pdf",
    width = 5,
    height = 6)
par(mfrow = c(2,1))
hist(ins_ratio,
     breaks = seq(0, round(max(ins_ratio) + 0.1, 2), by = 0.05),
     main = "",
     xlab = "Ratio insertions/revisions",
     cex.lab = 1.5,
     cex.axis = 1.3,
     cex.main = 1.5)

hist(del_ratio,
     breaks = seq(0, round(max(del_ratio) + 0.1, 2), by = 0.05),
     main = "",
     xlab = "Ratio deletions/revisions",
     cex.lab = 1.5,
     cex.axis = 1.3,
     cex.main = 1.5)
dev.off()
cat("File statistics2a.pdf printed!\n")

pdf("statistics2b.pdf")
hist(del_ins_ratio,
     breaks = seq(0, round(max(del_ins_ratio) + 0.1, 2), by = 0.05),
     main = "",
     xlab = "Ratio deletions/insertions",
     cex.lab = 1.5,
     cex.axis = 1.3,
     cex.main = 1.5)
dev.off()
cat("File statistics2b.pdf printed!\n")

cat("\nTotal number of pages,")
cat(nrow(data))
cat("\nTotal number of revisions,")
cat(sum(data$revs))
cat("\nAverage number of tokens,")
cat(floor(mean(data$avg_tokens)))
cat("\nAverage number of links,")
cat(floor(mean(data$links)))
cat("\nAverage number of revisions,")
cat(floor(mean(data$revs)))
cat("\n\n")
