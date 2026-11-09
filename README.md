# Smart API Rate Limiter & Key Swapper
## Overview
Handling frequent **429 Too Many Requests** errors is one of the biggest pain points for developers working with high-throughput external APIs. To solve this, this system acts as a production-grade **Smart API Rate Limiter and Key Swapper** designed to seamlessly manage $N$ number of API keys through intelligent allocation and robust health checking.
## Core Features & Architecture
* **Smart Key Pool & Rotation:** Manages a dynamic pool of API keys using a normal round-robin mechanism coupled with advanced health checking.
* **Automated Isolation & Quarantine:** If an API key triggers a 429 error or fails to respond, the system automatically isolates and temporarily shelves that key for a duration of **24 to 36 hours**. 
* **Self-Healing Loop:** If no valid response is received from an isolated key within the specified time frame, it is cleanly pruned from the active rotation loop.
* **Manual Override & Buffer Control:** Developers retain full administrative control—if desired, users can manually re-inject an isolated key back into the pool before the 24–36 hour buffer window officially expires.
* **Built-in SDK Buffer:** Efficiently handles burst traffic and request queuing down the line to ensure zero downtime and smooth upstream dispatch.
By addressing the core friction of API quota exhaustions and rate limits, this middleware ensures bulletproof reliability for backend applications and AI-driven workflows.
