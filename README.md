# LeetCode SQL Questions - PySpark Solutions-(BI Learner)
This repository is dedicated to solutions for LeetCode SQL questions implemented in PySpark.

This notebook contains a collection of SQL-related challenges sourced from **[LeetCode](https://leetcode.com/)**. The primary goal is to enhance SQL skills while leveraging **PySpark**, a powerful framework for large-scale data processing.

For each question, you will find:
- A brief problem description
- The relevant dataset or schema context
- A step-by-step solution implemented using PySpark DataFrames and SQL functions

### **Question Breakdown by Difficulty:**
| Difficulty | Count |
|------------|-------|
| Easy       | 111   |
| Medium     | 130   |
| Hard       | 59    |
| **Total**  | 300   |

All solutions in this notebook are implemented using **PySpark** to ensure consistency with big data processing practices. The approach utilizes **PySpark’s Python API, DataFrame API, and built-in functions** to solve problems efficiently.

> **Disclaimer:**  
> All questions and problem statements belong to **[LeetCode](https://leetcode.com/)**. This notebook is solely for educational purposes, and all credit and ownership of the questions remain with **[LeetCode](https://leetcode.com/)**.


**[175. Combine Two Tables (Easy)]()**

**[176. Second Highest Salary (Med)]()**

**[177. Nth Highest Salary (Med)]()**

**[178. Rank Scores (Med)]()**
**[180. Consecutive Numbers (Med)]()**
**[181. Employees Earning More Than Their Managers (Easy)]()**
**[182. Duplicate Emails (Easy)]()**
**[183. Customers Who Never Order (Easy)]()**
**[184. Department Highest Salary (Med)]()**
**[185. Department Top Three Salaries (Hard)]()**
**[196. Delete Duplicate Emails (Easy)]()**
**[197. Rising Temperature (Easy)]()**
**[262. Trips and Users (Hard)]()**
**[511. Game Play Analysis I (Easy)]()**
**[512. Game Play Analysis II (Easy)]()**
**[534. Game Play Analysis III (Med)]()**
**[550. Game Play Analysis IV (Med)]()**
**[569. Median Employee Salary (Hard)]()**
**[570. Managers with at Least 5 Direct Reports (Med)]()**
**[571. Find Median Given Frequency of Numbers (Hard)]()**
**[574. Winning Candidate (Med)]()**
**[577. Employee Bonus (Easy)]()**
**[578. Get Highest Answer Rate Question (Med)]()**
**[579. Find Cumulative Salary of an Employee (Hard)]()**
**[580. Count Student Number in Departments (Med)]()**
**[584. Find Customer Referee (Easy)]()**
**[585. Investments in 2016 (Med)]()**
**[586. Customer Placing the Largest Number of Orders (Easy)]()**
**[595. Big Countries (Easy)]()**
**[596. Classes More Than 5 Students (Easy)]()**
**[597. Friend Requests I: Overall Acceptance Rate (Easy)]()**
**[601. Human Traffic of Stadium (Hard)]()**
**[602. Friend Requests II: Who Has the Most Friends (Med)]()**
**[603. Consecutive Available Seats (Easy)]()**
**[607. Sales Person (Easy)]()**
**[608. Tree Node (Med)]()**
**[610. Triangle Judgement (Easy)]()**
**[612. Shortest Distance in a Plane (Med)]()**
**[613. Shortest Distance in a Line (Easy)]()**
**[614. Second Degree Follower (Med)]()**
**[615. Average Salary: Departments VS Company (Hard)]()**
**[618. Students Report By Geography (Hard)]()**
**[619. Biggest Single Number (Easy)]()**
**[620. Not Boring Movies (Easy)]()**
**[626. Exchange Seats (Med)]()**
**[627. Swap Salary (Easy)]()**
**[1045. Customers Who Bought All Products (Med)]()**
**[1050. Actors and Directors Who Cooperated At Least Three Times (Easy)]()**
**[1068. Product Sales Analysis I (Easy)]()**
**[1069. Product Sales Analysis II (Easy)]()**
**[1070. Product Sales Analysis III (Med)]()**
**[1075. Project Employees I (Easy)]()**
**[1076. Project Employees II (Easy)]()**
**[1077. Project Employees III (Med)]()**
**[1082. Sales Analysis I (Easy)]()**
**[1083. Sales Analysis II (Easy)]()**
**[1084. Sales Analysis III (Easy)]()**
**[1097. Game Play Analysis V (Hard)]()**
**[1098. Unpopular Books (Med)]()**
**[1107. New Users Daily Count (Med)]()**
**[1112. Highest Grade For Each Student (Med)]()**
**[1113. Reported Posts (Easy)]()**
**[1126. Active Businesses (Med)]()**
**[1127. User Purchase Platform (Hard)]()**
**[1132. Reported Posts II (Med)]()**
**[1141. User Activity for the Past 30 Days I (Easy)]()**
**[1142. User Activity for the Past 30 Days II (Easy)]()**
**[1148. Article Views I (Easy)]()**
**[1149. Article Views II (Med)]()**
**[1158. Market Analysis I (Med)]()**
**[1159. Market Analysis II (Hard)]()**
**[1164. Product Price at a Given Date (Med)]()**
**[1173. Immediate Food Delivery I (Easy)]()**
**[1174. Immediate Food Delivery II (Med)]()**
**[1179. Reformat Department Table (Easy)]()**
**[1193. Monthly Transactions I (Med)]()**
**[1194. Tournament Winners (Hard)]()**
**[1204. Last Person to Fit in the Bus (Med)]()**
**[1205. Monthly Transactions II (Med)]()**
**[1211. Queries Quality and Percentage (Easy)]()**
**[1212. Team Scores in Football Tournament (Med)]()**
**[1225. Report Contiguous Dates (Hard)]()**
**[1241. Number of Comments per Post (Easy)]()**
**[1251. Average Selling Price (Easy)]()**
**[1264. Page Recommendations (Med)]()**
**[1270. All People Report to the Given Manager (Med)]()**
**[1280. Students and Examinations (Easy)]()**
**[1285. Find the Start and End Number of Continuous Ranges (Med)]()**
**[1294. Weather Type in Each Country (Easy)]()**
**[1303. Find the Team Size (Easy)]()**
**[1308. Running Total for Different Genders (Med)]()**
**[1321. Restaurant Growth (Med)]()**
**[1322. Ads Performance (Easy)]()**
**[1327. List the Products Ordered in a Period (Easy)]()**
**[1336. Number of Transactions per Visit (Hard)]()**
**[1341. Movie Rating (Med)]()**
**[1350. Students With Invalid Departments (Easy)]()**
**[1355. Activity Participants (Med)]()**
**[1364. Number of Trusted Contacts of a Customer (Med)]()**
**[1369. Get the Second Most Recent Activity (Hard)]()**
**[1378. Replace Employee ID With The Unique Identifier (Easy)]()**
**[1384. Total Sales Amount by Year (Hard)]()**
**[1393. Capital Gain/Loss (Med)]()**
**[1398. Customers Who Bought Products A and B but Not C (Med)]()**
**[1407. Top Travellers (Easy)]()**
**[1412. Find the Quiet Students in All Exams (Hard)]()**
**[1421. NPV Queries (Easy)]()**
**[1435. Create a Session Bar Chart (Easy)]()**
**[1440. Evaluate Boolean Expression (Med)]()**
**[1445. Apples & Oranges (Med)]()**
**[1454. Active Users (Med)]()**
**[1459. Rectangles Area (Med)]()**
**[1468. Calculate Salaries (Med)]()**
**[1479. Sales by Day of the Week (Hard)]()**
**[1484. Group Sold Products By The Date (Easy)]()**
**[1495. Friendly Movies Streamed Last Month (Easy)]()**
**[1501. Countries You Can Safely Invest In (Med)]()**
**[1511. Customer Order Frequency (Easy)]()**
**[1517. Find Users With Valid E-Mails (Easy)]()**
**[1527. Patients With a Condition (Easy)]()**
**[1532. The Most Recent Three Orders (Med)]()**
**[1543. Fix Product Name Format (Easy)]()**
**[1549. The Most Recent Orders for Each Product (Med)]()**
**[1555. Bank Account Summary (Med)]()**
**[1565. Unique Orders and Customers Per Month (Easy)]()**
**[1571. Warehouse Manager (Easy)]()**
**[1581. Customer Who Visited but Did Not Make Any Transactions (Easy)]()**
**[1587. Bank Account Summary II (Easy)]()**
**[1596. The Most Frequently Ordered Products for Each Customer (Med)]()**
**[1607. Sellers With No Sales (Easy)]()**
**[1613. Find the Missing IDs (Med)]()**
**[1623. All Valid Triplets That Can Represent a Country (Easy)]()**
**[1633. Percentage of Users Attended a Contest (Easy)]()**
**[1635. Hopper Company Queries I (Hard)]()**
**[1645. Hopper Company Queries II (Hard)]()**
**[1651. Hopper Company Queries III (Hard)]()**
**[1661. Average Time of Process per Machine (Easy)]()**
**[1667. Fix Names in a Table (Easy)]()**
**[1677. Product's Worth Over Invoices (Easy)]()**
**[1683. Invalid Tweets (Easy)]()**
**[1693. Daily Leads and Partners (Easy)]()**
**[1699. Number of Calls Between Two Persons (Med)]()**
**[1709. Biggest Window Between Visits (Med)]()**
**[1715. Count Apples and Oranges (Med)]()**
**[1729. Find Followers Count (Easy)]()**
**[1731. The Number of Employees Which Report to Each Employee (Easy)]()**
**[1741. Find Total Time Spent by Each Employee (Easy)]()**
**[1747. Leetflex Banned Accounts (Med)]()**
**[1757. Recyclable and Low Fat Products (Easy)]()**
**[1767. Find the Subtasks That Did Not Execute (Hard)]()**
**[1777. Product's Price for Each Store (Easy)]()**
**[1783. Grand Slam Titles (Med)]()**
**[1789. Primary Department for Each Employee (Easy)]()**
**[1795. Rearrange Products Table (Easy)]()**
**[1809. Ad-Free Sessions (Easy)]()**
**[1811. Find Interview Candidates (Med)]()**
**[1821. Find Customers With Positive Revenue this Year (Easy)]()**
**[1831. Maximum Transaction Each Day (Med)]()**
**[1841. League Statistics (Med)]()**
**[1843. Suspicious Bank Accounts (Med)]()**
**[1853. Convert Date Format (Easy)]()**
**[1867. Orders With Maximum Quantity Above Average (Med)]()**
**[1873. Calculate Special Bonus (Easy)]()**
**[1875. Group Employees of the Same Salary (Med)]()**
**[1890. The Latest Login in 2020 (Easy)]()**
**[1892. Page Recommendations II (Hard)]()**
**[1907. Count Salary Categories (Med)]()**
**[1917. Leetcodify Friends Recommendations (Hard)]()**
**[1919. Leetcodify Similar Friends (Hard)]()**
**[1934. Confirmation Rate (Med)]()**
**[1939. Users That Actively Request Confirmation Messages (Easy)]()**
**[1949. Strong Friendship (Med)]()**
**[1951. All the Pairs With the Maximum Number of Common Followers (Med)]()**
**[1965. Employees With Missing Information (Easy)]()**
**[1972. First and Last Call On the Same Day (Hard)]()**
**[1978. Employees Whose Manager Left the Company (Easy)]()**
**[1988. Find Cutoff Score for Each School (Med)]()**
**[1990. Count the Number of Experiments (Med)]()**
**[2004. The Number of Seniors and Juniors to Join the Company (Hard)]()**
**[2010. The Number of Seniors and Juniors to Join the Company II (Hard)]()**
**[2020. Number of Accounts That Did Not Stream (Med)]()**
**[2026. Low-Quality Problems (Easy)]()**
**[2041. Accepted Candidates From the Interviews (Med)]()**
**[2051. The Category of Each Member in the Store (Med)]()**
**[2066. Account Balance (Med)]()**
**[2072. The Winner University (Easy)]()**
**[2082. The Number of Rich Customers (Easy)]()**
**[2084. Drop Type 1 Orders for Customers With Type 0 Orders (Med)]()**
**[2112. The Airport With the Most Traffic (Med)]()**
**[2118. Build the Equation (Hard)]()**
**[2142. The Number of Passengers in Each Bus I (Med)]()**
**[2153. The Number of Passengers in Each Bus II (Hard)]()**
**[2159. Order Two Columns Independently (Med)]()**
**[2173. Longest Winning Streak (Hard)]()**
**[2175. The Change in Global Rankings (Med)]()**
**[2199. Finding the Topic of Each Post (Hard)]()**
**[2205. The Number of Users That Are Eligible for Discount (Easy)]()**
**[2228. Users With Two Purchases Within Seven Days (Med)]()**
**[2230. The Users That Are Eligible for Discount (Easy)]()**
**[2238. Number of Times a Driver Was a Passenger (Med)]()**
**[2252. Dynamic Pivoting of a Table (Hard)]()**
**[2253. Dynamic Unpivoting of a Table (Hard)]()**
**[2292. Products With Three or More Orders in Two Consecutive Years (Med)]()**
**[2298. Tasks Count in the Weekend (Med)]()**
**[2308. Arrange Table by Gender (Med)]()**
**[2314. The First Day of the Maximum Recorded Degree in Each City (Med)]()**
**[2324. Product Sales Analysis IV (Med)]()**
**[2329. Product Sales Analysis V (Easy)]()**
**[2339. All the Matches of the League (Easy)]()**
**[2346. Compute the Rank as a Percentage (Med)]()**
**[2356. Number of Unique Subjects Taught by Each Teacher (Easy)]()**
**[2362. Generate the Invoice (Hard)]()**
**[2372. Calculate the Influence of Each Salesperson (Med)]()**
**[2377. Sort the Olympic Table (Easy)]()**
**[2388. Change Null Values in a Table to the Previous Value (Med)]()**
**[2394. Employees With Deductions (Med)]()**
**[2474. Customers With Strictly Increasing Purchases (Hard)]()**
**[2480. Form a Chemical Bond (Easy)]()**
**[2494. Merge Overlapping Events in the Same Hall (Hard)]()**
**[2504. Concatenate the Name and the Profession (Easy)]()**
**[2668. Find Latest Salaries (Easy)]()**
**[2669. Count Artist Occurrences On Spotify Ranking List (Easy)]()**
**[2686. Immediate Food Delivery III (Med)]()**
**[2687. Bikes Last Time Used (Easy)]()**
**[2688. Find Active Users (Med)]()**
**[2701. Consecutive Transactions with Increasing Amounts (Hard)]()**
**[2720. Popularity Percentage (Hard)]()**
**[2738. Count Occurrences in Text (Med)]()**
**[2752. Customers with Maximum Number of Transactions on Consecutive Days (Hard)]()**
**[2783. Flight Occupancy and Waitlist Analysis (Med)]()**
**[2837. Total Traveled Distance (Easy)]()**
**[2853. Highest Salaries Difference (Easy)]()**
**[2854. Rolling Average Steps (Med)]()**
**[2893. Calculate Orders Within Each Interval (Med)]()**
**[2922. Market Analysis III (Med)]()**
**[2978. Symmetric Coordinates (Med)]()**
**[2984. Find Peak Calling Hours for Each City (Med)]()**
**[2985. Calculate Compressed Mean (Easy)]()**
**[2986. Find Third Transaction (Med)]()**
**[2987. Find Expensive Cities (Easy)]()**
**[2988. Manager of the Largest Department (Med)]()**
**[2989. Class Performance (Med)]()**
**[2990. Loan Types (Easy)]()**
**[2991. Top Three Wineries (Hard)]()**
**[2993. Friday Purchases I (Med)]()**
**[2994. Friday Purchases II (Hard)]()**
**[2995. Viewers Turned Streamers (Hard)]()**
**[3050. Pizza Toppings Cost Analysis (Med)]()**
**[3051. Find Candidates for Data Scientist Position (Easy)]()**
**[3052. Maximize Items (Hard)]()**
**[3053. Classifying Triangles by Lengths (Easy)]()**
**[3054. Binary Tree Nodes (Med)]()**
**[3055. Top Percentile Fraud (Med)]()**
**[3056. Snaps Analysis (Med)]()**
**[3057. Employees Project Allocation (Hard)]()**
**[3058. Friends With No Mutual Friends (Med)]()**
**[3059. Find All Unique Email Domains (Easy)]()**
**[3060. User Activities within Time Bounds (Hard)]()**
**[3061. Calculate Trapping Rain Water (Hard)]()**
**[3087. Find Trending Hashtags (Med)]()**
**[3089. Find Bursty Behavior (Med)]()**
**[3103. Find Trending Hashtags II (Hard)]()**
**[3118. Friday Purchase III (Med)]()**
**[3124. Find Longest Calls (Med)]()**
**[3126. Server Utilization Time (Med)]()**
**[3140. Consecutive Available Seats II (Med)]()**
**[3150. Invalid Tweets II (Easy)]()**
**[3156. Employee Task Duration and Concurrent Tasks (Hard)]()**
**[3166. Calculate Parking Fees and Duration (Med)]()**
**[3172. Second Day Verification (Easy)]()**
**[3182. Find Top Scoring Students (Med)]()**
**[3188. Find Top Scoring Students II (Hard)]()**
**[3198. Find Cities in Each State (Easy)]()**
**[3204. Bitwise User Permissions Analysis (Med)]()**
**[3214. Year on Year Growth Rate (Hard)]()**
**[3220. Odd and Even Transactions (Med)]()**
**[3230. Customer Purchasing Behavior Analysis (Med)]()**
**[3236. CEO Subordinate Hierarchy (Hard)]()**
**[3246. Premier League Table Ranking (Easy)]()**
**[3252. Premier League Table Ranking II (Med)]()**
**[3262. Find Overlapping Shifts (Med)]()**
**[3268. Find Overlapping Shifts II (Hard)]()**
**[3278. Find Candidates for Data Scientist Position II (Med)]()**
**[3293. Calculate Product Final Price (Med)]()**
**[3308. Find Top Performing Driver (Med)]()**
**[3322. Premier League Table Ranking III (Med)]()**
**[3328. Find Cities in Each State II (Med)]()**
**[3338. Second Highest Salary II (Med)]()**
**[3358. Books with NULL Ratings (Easy)]()**
**[3368. First Letter Capitalization (Hard)]()**
**[3374. First Letter Capitalization II (Hard)]()**
**[3384. Team Dominance by Pass Success (Hard)]()**
**[3390. Longest Team Pass Streak (Hard)]()**
**[3401. Find Circular Gift Exchange Chains (Hard)]()**
**[3415. Find Products with Three Consecutive Digits (Easy)]()**
**[3421. Find Students Who Improved (Med)]()**
**[3436. Find Valid Emails (Easy)]()**
**[3451. Find Invalid IP Addresses (Hard)]()**
**[3465. Find Products with Valid Serial Numbers (Easy)]()**
**[3475. DNA Pattern Recognition (Med)]()**
![image](https://github.com/user-attachments/assets/c7bfc53c-cdb5-4ea0-b47b-abdbab97d3a1)
